// Package eventbus implements a simple event bus.
package eventbus

import "sync"

type Event interface {
	GetType() string
}

type Subscriber struct {
	ch chan<- Event
	wg *sync.WaitGroup
}

func NewSubscriber(ch chan<- Event, wg *sync.WaitGroup) *Subscriber {
	return &Subscriber{
		ch: ch,
		wg: wg,
	}
}

type EventBusIf interface {
	Subscribe(chan<- Event)
	Publish(Event)
}

type EventBus struct {
	subscribers []Subscriber
}

func NewEventBus() *EventBus {
	return &EventBus{
		subscribers: []Subscriber{},
	}
}

func (e *EventBus) Subscribe(s Subscriber) {
	e.subscribers = append(e.subscribers, s)
}

func (e *EventBus) Publish(msg Event) {
	for _, s := range e.subscribers {
		s.wg.Add(1)
		s.ch <- msg
	}
}

// GlobalEventBus instance
var GlobalEventBus = NewEventBus()
