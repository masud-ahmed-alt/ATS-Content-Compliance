package lib

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"strings"
	"time"
)

// AnalyzerClient handles sending batches to the analyzer with retry logic
type AnalyzerClient struct {
	httpClient      *http.Client
	analyzerURL     string
	useGzip         bool
	maxRetries      int
	retryBackoff    time.Duration
	eventHub        *EventHub // For publishing failure events
}

// NewAnalyzerClient creates a new analyzer client
func NewAnalyzerClient(httpClient *http.Client, analyzerURL string, useGzip bool) *AnalyzerClient {
	return &AnalyzerClient{
		httpClient:      httpClient,
		analyzerURL:     analyzerURL,
		useGzip:         useGzip,
		maxRetries:      3,
		retryBackoff:    2 * time.Second,
		eventHub:        nil,
	}
}

func (ac *AnalyzerClient) SetEventHub(eh *EventHub) {
	ac.eventHub = eh
}

// ================================
// PUBLIC ENTRYPOINT
// ================================
func (ac *AnalyzerClient) SendBatch(batch PageBatch) error {
	var lastErr error
	ctx := context.Background()

	for attempt := 0; attempt <= ac.maxRetries; attempt++ {

		err := ac.sendBatchOnce(ctx, batch)
		if err == nil {
			if attempt > 0 {
				log.Printf("[analyzer_client] Batch %s succeeded on retry #%d", batch.RequestID, attempt)
			}
			return nil
		}

		lastErr = err

		if attempt < ac.maxRetries {
			backoff := ac.retryBackoff * time.Duration(1<<uint(attempt))
			log.Printf("[analyzer_client:warning] Batch %s failed attempt %d/%d: %v — retrying in %v",
				batch.RequestID, attempt+1, ac.maxRetries+1, err, backoff)
			time.Sleep(backoff)
		}
	}

	errorMsg := fmt.Sprintf("Failed to deliver batch %s after %d retries: %v",
		batch.RequestID, ac.maxRetries+1, lastErr)

	log.Printf("[analyzer_client:error] %s", errorMsg)

	if ac.eventHub != nil {
		ac.eventHub.Publish(ProgressEvent{
			Type:      "batch_delivery_failed",
			RequestID: batch.RequestID,
			Message:   errorMsg,
		})
	}

	return lastErr
}

// ================================
// SINGLE ATTEMPT — STREAMING UPLOAD
// ================================
func (ac *AnalyzerClient) sendBatchOnce(ctx context.Context, batch PageBatch) error {

	// Pipe to stream JSON → gzip → HTTP request body
	pr, pw := io.Pipe()

	var gw *gzip.Writer
	if ac.useGzip {
		gw = gzip.NewWriter(pw)
	}

	// Encode JSON in a background goroutine
	go func() {
		var enc *json.Encoder

		if ac.useGzip {
			enc = json.NewEncoder(gw)
		} else {
			enc = json.NewEncoder(pw)
		}

		err := enc.Encode(batch)

		if ac.useGzip {
			_ = gw.Close()
		}
		_ = pw.CloseWithError(err)
	}()

	// Create HTTP request
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, ac.analyzerURL, pr)
	if err != nil {
		_ = pr.Close()
		return fmt.Errorf("request creation failed: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Connection", "close")
	if ac.useGzip {
		req.Header.Set("Content-Encoding", "gzip")
	}

	// Execute request
	resp, err := ac.httpClient.Do(req)
	if err != nil {
		_ = pr.Close()

		// Retry only transient errors
		if isTransientNetErr(err) {
			return fmt.Errorf("transient http error: %w", err)
		}
		return fmt.Errorf("http request failed: %w", err)
	}

	defer resp.Body.Close()

	respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 5*1024*1024))

	// Accept 202 (Accepted) as success - analyzer returns 202 for async processing
	if resp.StatusCode == 202 {
		log.Printf("[analyzer_client] Batch %s accepted for async processing (202)", batch.RequestID)
		return nil
	}

	if resp.StatusCode >= 400 {
		return fmt.Errorf("analyzer returned HTTP %d: %s", resp.StatusCode, string(respBody))
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	log.Printf("[analyzer_client] Batch %s delivered successfully", batch.RequestID)
	return nil
}

// ================================
// TRANSIENT ERROR DETECTOR
// ================================
func isTransientNetErr(err error) bool {
	if err == nil {
		return false
	}
	if ne, ok := err.(net.Error); ok && (ne.Temporary() || ne.Timeout()) {
		return true
	}

	msg := err.Error()
	return strings.Contains(msg, "connection reset") ||
		strings.Contains(msg, "unexpected EOF") ||
		strings.Contains(msg, "broken pipe") ||
		strings.Contains(msg, "timeout") ||
		strings.Contains(msg, "EOF")
}
