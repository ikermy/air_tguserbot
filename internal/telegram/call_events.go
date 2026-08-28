package telegram

import (
	"context"
	"sync"
	"time"
)

// Call is the Telegram equivalent of the WhatsApp call handle exposed by RPC.
type Call struct{ id string }

func (c *Call) ID() string {
	if c == nil {
		return ""
	}
	return c.id
}

type CallEvent struct {
	CallID     string
	Sequence   uint64
	Timestamp  time.Time
	Type       string
	Delta      string
	Text       string
	ResponseID string
	Reason     string
	Err        error
}

type callEventHub struct {
	mu      sync.Mutex
	history []CallEvent
	subs    map[chan CallEvent]struct{}
	closed  bool
}

func newCallEventHub() *callEventHub { return &callEventHub{subs: make(map[chan CallEvent]struct{})} }

func (h *callEventHub) publish(event CallEvent) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return
	}
	event.Sequence = uint64(len(h.history) + 1)
	if event.Timestamp.IsZero() {
		event.Timestamp = time.Now()
	}
	h.history = append(h.history, event)
	for ch := range h.subs {
		select {
		case ch <- event:
		default:
		}
	}
}

func (h *callEventHub) subscribe(ctx context.Context, after uint64) (<-chan CallEvent, error) {
	h.mu.Lock()
	ch := make(chan CallEvent, 64)
	for _, event := range h.history {
		if event.Sequence > after {
			ch <- event
		}
	}
	if h.closed {
		close(ch)
		h.mu.Unlock()
		return ch, nil
	}
	h.subs[ch] = struct{}{}
	h.mu.Unlock()
	go func() {
		<-ctx.Done()
		h.mu.Lock()
		if _, ok := h.subs[ch]; ok {
			delete(h.subs, ch)
			close(ch)
		}
		h.mu.Unlock()
	}()
	return ch, nil
}

func (h *callEventHub) close() {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return
	}
	h.closed = true
	for ch := range h.subs {
		close(ch)
	}
	h.subs = nil
}
