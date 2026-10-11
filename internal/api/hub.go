package api

import (
	"sync"

	"github.com/seahyc/switchboard-oss/internal/event"
)

// Hub is the push event stream fan-out. The daemon registers it as a
// pipeline.Sink; every newly-ingested message is broadcast to all connected
// SSE subscribers (the agent's proactive feed). Slow subscribers are dropped
// rather than allowed to block ingestion.
type Hub struct {
	mu   sync.RWMutex
	subs map[chan event.Message]struct{}
}

// NewHub creates an empty Hub.
func NewHub() *Hub {
	return &Hub{subs: map[chan event.Message]struct{}{}}
}

// OnIngest implements pipeline.Sink — broadcast to subscribers, never block.
func (h *Hub) OnIngest(m event.Message) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	for ch := range h.subs {
		select {
		case ch <- m:
		default:
			// Subscriber is behind; drop this event for them rather than stall.
		}
	}
}

// Subscribe returns a channel of events and an unsubscribe func.
func (h *Hub) Subscribe() (<-chan event.Message, func()) {
	ch := make(chan event.Message, 64)
	h.mu.Lock()
	h.subs[ch] = struct{}{}
	h.mu.Unlock()
	return ch, func() {
		h.mu.Lock()
		if _, ok := h.subs[ch]; ok {
			delete(h.subs, ch)
			close(ch)
		}
		h.mu.Unlock()
	}
}
