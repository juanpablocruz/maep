package testutil

import (
	"context"
	"sync"
	"time"

	"github.com/juanpablocruz/maep/pkg/node"
)

// EventCollector subscribes to a node's event stream and buffers events
// for deterministic assertions in tests without racing on channel close.
type EventCollector struct {
	ch     chan node.Event
	notify chan struct{}

	mu  sync.Mutex
	buf []node.Event

	cancel context.CancelFunc
}

func NewEventCollector(buffer int) *EventCollector {
	return &EventCollector{
		ch:     make(chan node.Event, buffer),
		notify: make(chan struct{}, 1),
	}
}

// Attach connects the collector to the node and starts the buffering loop.
func (ec *EventCollector) Attach(n *node.Node) {
	ctx, cancel := context.WithCancel(context.Background())
	ec.cancel = cancel
	n.AttachEvents(ec.ch)
	go ec.loop(ctx)
}

// Detach stops the buffering loop.
// It intentionally does not mutate the node's Events field to avoid data races
// with concurrent emitters. The node will drop events if the channel fills.
func (ec *EventCollector) Detach(_ *node.Node) {
	if ec.cancel != nil {
		ec.cancel()
	}
}

func (ec *EventCollector) loop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case e := <-ec.ch:
			ec.mu.Lock()
			ec.buf = append(ec.buf, e)
			// coalesce notifications
			select {
			case ec.notify <- struct{}{}:
			default:
			}
			ec.mu.Unlock()
		}
	}
}

// Snapshot returns a copy of buffered events.
func (ec *EventCollector) Snapshot() []node.Event {
	ec.mu.Lock()
	defer ec.mu.Unlock()
	out := make([]node.Event, len(ec.buf))
	copy(out, ec.buf)
	return out
}

// WaitFor waits up to timeout for pred to be satisfied by the buffered events.
func (ec *EventCollector) WaitFor(timeout time.Duration, pred func([]node.Event) bool) bool {
	deadline := time.Now().Add(timeout)
	for {
		ec.mu.Lock()
		ok := pred(ec.buf)
		ec.mu.Unlock()
		if ok {
			return true
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return false
		}
		select {
		case <-ec.notify:
			// new event, re-check
		case <-time.After(remaining):
			return false
		}
	}
}
