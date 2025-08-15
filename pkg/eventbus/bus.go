// Package eventbus implements a simple event bus.
package eventbus

import "sync"

type Event interface {
	GetType() string
}

type Subscriber interface {
	OnEvent(Event)
	GetChannel() chan Event
	GetWaitGroup() *sync.WaitGroup
}

type EventBusIf interface {
	Subscribe(chan<- Event)
	Publish(Event)
}

type EventBus struct {
	subscribers []Subscriber
	started     bool
	mu          sync.RWMutex // Protect started field
}

func NewEventBus() *EventBus {
	return &EventBus{
		subscribers: []Subscriber{},
		started:     false,
	}
}

func (e *EventBus) Subscribe(s Subscriber) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.subscribers = append(e.subscribers, s)
}

func (e *EventBus) Publish(msg Event) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	if !e.started {
		// If not started, just return - events will be lost
		return
	}

	for _, s := range e.subscribers {
		s.GetWaitGroup().Add(1)
		s.GetChannel() <- msg
	}
}

func (e *EventBus) Start() {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.started {
		return
	}

	for _, s := range e.subscribers {
		go func(subscriber Subscriber) {
			for event := range subscriber.GetChannel() {
				subscriber.OnEvent(event)
				subscriber.GetWaitGroup().Done()
			}
		}(s)
	}
	e.started = true
}

// GlobalEventBus instance
var GlobalEventBus = NewEventBus()
