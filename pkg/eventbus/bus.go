// Package eventbus provides a simple, concurrent event bus with fanout
// delivery and precise "all processed" synchronization.
package eventbus

import (
	"context"
	"sync"
)

// Event is any message published to the bus.
type Event interface{ GetType() string }

// Subscriber consumes events on its own channel.
// NOTE: The bus will start a goroutine that reads from GetChannel()
// and calls OnEvent(ev) for each event. Do NOT close the channel.
type Subscriber interface {
	OnEvent(Event)
	GetChannel() chan Event // ideally buffered if OnEvent can be slow
}

type EventBusIf interface {
	Subscribe(Subscriber)
	Publish(Event)
	Start()             // idempotent
	Stop()              // idempotent; drains and shuts down cleanly
	WaitForProcessing() // blocks until every published event has been OnEvent'ed
}

// Option configures the Bus.
type Option func(*Bus)

// WithPublishBuffer sets the internal publish queue capacity.
func WithPublishBuffer(n int) Option {
	return func(b *Bus) {
		if n < 1 {
			n = 1
		}
		b.pubCh = make(chan delivery, n)
	}
}

// Bus is a concrete implementation of EventBusIf.
type Bus struct {
	// subscribers
	subsMu sync.RWMutex
	subs   map[Subscriber]struct{}

	// publish queue (fanout source)
	pubCh chan delivery

	// lifecycle
	startOnce sync.Once
	stopOnce  sync.Once
	started   bool

	// context for subscriber worker shutdown
	ctx    context.Context
	cancel context.CancelFunc

	// goroutine management
	fanoutWG sync.WaitGroup // fanout loop
	subsWG   sync.WaitGroup // per-subscriber workers

	// processing accounting: one count per (event, subscriber) delivery
	procWG sync.WaitGroup
}

type delivery struct {
	ev      Event
	targets []Subscriber
}

// New creates a Bus. If no options are provided, a sensible default is used.
func New(opts ...Option) *Bus {
	b := &Bus{
		subs: make(map[Subscriber]struct{}),
		// default internal publish queue size
		pubCh: make(chan delivery, 1024),
	}
	for _, o := range opts {
		o(b)
	}
	return b
}

// Subscribe registers a Subscriber. If the bus is already started,
// a worker goroutine is spun up to consume the subscriber's channel
// and call OnEvent.
func (b *Bus) Subscribe(s Subscriber) {
	b.subsMu.Lock()
	if _, exists := b.subs[s]; exists {
		b.subsMu.Unlock()
		return
	}
	b.subs[s] = struct{}{}
	b.subsMu.Unlock()

	// If already started, begin consuming this subscriber's channel.
	if b.isStarted() {
		b.startSubscriberWorker(s)
	}
}

// Publish enqueues an event to be delivered to the *current* snapshot
// of subscribers. It increments the processing counter BEFORE enqueueing
// so WaitForProcessing() observes all work.
func (b *Bus) Publish(ev Event) {
	if !b.isStarted() {
		// If not started, do nothing (idempotent semantics).
		return
	}

	// Snapshot subscribers.
	b.subsMu.RLock()
	targets := make([]Subscriber, 0, len(b.subs))
	for s := range b.subs {
		targets = append(targets, s)
	}
	b.subsMu.RUnlock()

	if len(targets) == 0 {
		return
	}

	// Account processing up-front for each target delivery.
	b.procWG.Add(len(targets))

	// Enqueue for fanout. This will block if the publish queue is full,
	// which provides backpressure upstream.
	select {
	case b.pubCh <- delivery{ev: ev, targets: targets}:
		// ok
	default:
		// If full, block rather than drop to guarantee delivery.
		b.pubCh <- delivery{ev: ev, targets: targets}
	}
}

// Start launches the fanout loop and subscriber workers. Idempotent.
func (b *Bus) Start() {
	b.startOnce.Do(func() {
		b.ctx, b.cancel = context.WithCancel(context.Background())
		b.setStarted(true)

		// Start subscriber workers for any pre-registered subs.
		b.subsMu.RLock()
		for s := range b.subs {
			b.startSubscriberWorker(s)
		}
		b.subsMu.RUnlock()

		// Fanout loop.
		b.fanoutWG.Add(1)
		go func() {
			defer b.fanoutWG.Done()
			for d := range b.pubCh {
				// Deliver to each target's channel. This blocks if
				// the target channel is full/unbuffered.
				for _, s := range d.targets {
					select {
					case s.GetChannel() <- d.ev:
						// per-subscriber worker will Do b.procWG.Done()
						// after OnEvent returns (see handleEvent).
					case <-b.ctx.Done():
						// Bus stopping: in normal Stop(), we don't cancel
						// until after all processing completes, so this path
						// shouldn't run. As a safety net, mark as done.
						b.procWG.Done()
					}
				}
			}
		}()
	})
}

// Stop drains, waits for all in-flight events to be processed,
// then cleanly shuts down workers. Idempotent.
func (b *Bus) Stop() {
	b.stopOnce.Do(func() {
		// Close publish intake to end fanout once queue is drained.
		close(b.pubCh)
		// Wait fanout loop to finish pushing remaining deliveries.
		b.fanoutWG.Wait()

		// Wait until every (event, subscriber) has completed OnEvent.
		b.procWG.Wait()

		// Now cancel workers and wait them out.
		if b.cancel != nil {
			b.cancel()
		}
		b.subsWG.Wait()

		// Mark not started for completeness (not re-startable with same instance).
		b.setStarted(false)
	})
}

// WaitForProcessing blocks until all published events (to the snapshot of
// subscribers at Publish time) have been processed by their OnEvent handlers.
func (b *Bus) WaitForProcessing() {
	b.procWG.Wait()
}

// --- internals ---

func (b *Bus) startSubscriberWorker(s Subscriber) {
	ch := s.GetChannel()
	b.subsWG.Add(1)
	go func() {
		defer b.subsWG.Done()
		for {
			select {
			case ev, ok := <-ch:
				if !ok {
					// IMPORTANT: subscribers must NOT close their channel.
					// If they do, we stop consuming to avoid panics.
					return
				}
				b.handleEvent(s, ev)
			case <-b.ctx.Done():
				return
			}
		}
	}()
}

func (b *Bus) handleEvent(s Subscriber, ev Event) {
	// Ensure we always decrement processing, even on panic.
	defer b.procWG.Done()
	defer func() {
		if recover() != nil {
			// swallow subscriber panics to avoid wedging the bus.
			// (Optionally log here.)
		}
	}()
	s.OnEvent(ev)
}

func (b *Bus) isStarted() bool {
	return b.started
}

func (b *Bus) setStarted(v bool) {
	b.started = v
}
