package lib

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
)

// Handler contains HTTP handlers
type Handler struct {
	crawler        *Crawler
	eventHub       *EventHub
	semaphore      chan struct{}
	activeRequests sync.Map
}

// NewHandler creates a new handler
func NewHandler(crawler *Crawler, eventHub *EventHub, maxConcurrent int) *Handler {
	return &Handler{
		crawler:   crawler,
		eventHub:  eventHub,
		semaphore: make(chan struct{}, maxConcurrent),
	}
}

type ActiveRequest struct {
	RequestID string    `json:"request_id"`
	StartedAt time.Time `json:"started_at"`
	UrlCount  int       `json:"url_count"`
}

// HandleFetch handles fetch requests
func (h *Handler) HandleFetch(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	defer r.Body.Close()

	var req FetchRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || len(req.Urls) == 0 {
		http.Error(w, "invalid JSON or empty urls", http.StatusBadRequest)
		return
	}

	requestID := uuid.NewString()
	h.activeRequests.Store(requestID, ActiveRequest{
		RequestID: requestID,
		StartedAt: time.Now().UTC(),
		UrlCount:  len(req.Urls),
	})

	go func() {
		h.semaphore <- struct{}{}
		defer func() { <-h.semaphore }()
		h.crawler.StartCrawl(requestID, req.Urls)
		h.activeRequests.Delete(requestID)
	}()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"status":     "started",
		"request_id": requestID,
	})
}

// HandleSSEAll handles SSE for all requests
func (h *Handler) HandleSSEAll(w http.ResponseWriter, r *http.Request) {
	h.streamSSE(w, r, "")
}

// HandleSSEByRequest handles SSE for a specific request
func (h *Handler) HandleSSEByRequest(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimPrefix(r.URL.Path, "/events/")
	if id == "" {
		http.Error(w, "missing request_id", http.StatusBadRequest)
		return
	}
	h.streamSSE(w, r, id)
}

// streamSSE streams SSE events
func (h *Handler) streamSSE(w http.ResponseWriter, r *http.Request, requestID string) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "stream unsupported", http.StatusInternalServerError)
		return
	}

	sub := h.eventHub.Subscribe(requestID)
	defer h.eventHub.Unsubscribe(requestID, sub)

	bw := bufio.NewWriter(w)
	fmt.Fprintf(bw, "event: connected\ndata: {}\n\n")
	bw.Flush()
	flusher.Flush()

	for {
		select {
		case <-r.Context().Done():
			return
		case ev := <-sub.ch:
			data, _ := json.Marshal(ev)
			fmt.Fprintf(bw, "event: %s\ndata: %s\n\n", ev.Type, data)
			bw.Flush()
			flusher.Flush()
		}
	}
}

// HandleActiveRequests returns the currently running requests
func (h *Handler) HandleActiveRequests(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var requests []ActiveRequest
	h.activeRequests.Range(func(_, value any) bool {
		if ar, ok := value.(ActiveRequest); ok {
			requests = append(requests, ar)
		}
		return true
	})

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"requests": requests,
	})
}
