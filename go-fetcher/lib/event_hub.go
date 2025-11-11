package lib

import (
	"sync"
)

// Subscriber represents a client subscribed to events
type Subscriber struct {
	ch   chan ProgressEvent
	done chan struct{}
}

// EventHub manages SSE subscriptions
type EventHub struct {
	mu        sync.RWMutex
	requestCh map[string]map[*Subscriber]struct{} // "" = global subscribers
}

// NewEventHub creates a new event hub
func NewEventHub() *EventHub {
	return &EventHub{requestCh: make(map[string]map[*Subscriber]struct{})}
}

// Subscribe adds a subscriber to the hub
func (h *EventHub) Subscribe(requestID string) *Subscriber {
	h.mu.Lock()
	defer h.mu.Unlock()
	s := &Subscriber{ch: make(chan ProgressEvent, 256), done: make(chan struct{})}
	if _, ok := h.requestCh[requestID]; !ok {
		h.requestCh[requestID] = make(map[*Subscriber]struct{})
	}
	h.requestCh[requestID][s] = struct{}{}
	return s
}

// Unsubscribe removes a subscriber from the hub
func (h *EventHub) Unsubscribe(requestID string, s *Subscriber) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if subs, ok := h.requestCh[requestID]; ok {
		delete(subs, s)
		close(s.ch)
		close(s.done)
		if len(subs) == 0 {
			delete(h.requestCh, requestID)
		}
	}
}

// Publish sends an event to all subscribers
func (h *EventHub) Publish(ev ProgressEvent) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	for _, subs := range []map[*Subscriber]struct{}{
		h.requestCh[""], // global
		h.requestCh[ev.RequestID],
	} {
		for s := range subs {
			select {
			case s.ch <- ev:
			default: // drop if slow
			}
		}
	}
}
