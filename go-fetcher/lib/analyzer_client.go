package lib

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"io"
	"net/http"
)

// AnalyzerClient handles sending batches to the analyzer
type AnalyzerClient struct {
	httpClient *http.Client
	analyzerURL string
	useGzip    bool
}

// NewAnalyzerClient creates a new analyzer client
func NewAnalyzerClient(httpClient *http.Client, analyzerURL string, useGzip bool) *AnalyzerClient {
	return &AnalyzerClient{
		httpClient:  httpClient,
		analyzerURL: analyzerURL,
		useGzip:     useGzip,
	}
}

// SendBatch sends a page batch to the analyzer
func (ac *AnalyzerClient) SendBatch(batch PageBatch) {
	var buf bytes.Buffer
	var body io.Reader

	if ac.useGzip {
		gzw := gzip.NewWriter(&buf)
		_ = json.NewEncoder(gzw).Encode(batch)
		_ = gzw.Close()
		body = bytes.NewReader(buf.Bytes())
	} else {
		_ = json.NewEncoder(&buf).Encode(batch)
		body = bytes.NewReader(buf.Bytes())
	}

	req, _ := http.NewRequest(http.MethodPost, ac.analyzerURL, body)
	req.Header.Set("Content-Type", "application/json")
	if ac.useGzip {
		req.Header.Set("Content-Encoding", "gzip")
	}
	resp, err := ac.httpClient.Do(req)
	if err == nil && resp != nil {
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}
}
