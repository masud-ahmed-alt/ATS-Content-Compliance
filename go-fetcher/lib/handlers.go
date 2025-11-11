package lib

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/google/uuid"
)

// Handler contains HTTP handlers
type Handler struct {
	crawler   *Crawler
	eventHub  *EventHub
	semaphore chan struct{}
}

// NewHandler creates a new handler
func NewHandler(crawler *Crawler, eventHub *EventHub, maxConcurrent int) *Handler {
	return &Handler{
		crawler:   crawler,
		eventHub:  eventHub,
		semaphore: make(chan struct{}, maxConcurrent),
	}
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
	go func() {
		h.semaphore <- struct{}{}
		defer func() { <-h.semaphore }()
		h.crawler.StartCrawl(requestID, req.Urls)
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
