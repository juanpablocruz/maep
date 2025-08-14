package eventbus

type Event interface {
	GetType() string
}

type EventBusIf interface {
	Subscribe(chan<- Event)
	Publish(Event)
}

type EventBus struct {
	subscribers []chan<- Event
}

func NewEventBus() *EventBus {
	return &EventBus{
		subscribers: []chan<- Event{},
	}
}

func (e *EventBus) Subscribe(ch chan<- Event) {
	e.subscribers = append(e.subscribers, ch)
}

func (e *EventBus) Publish(msg Event) {
	for _, ch := range e.subscribers {
		ch <- msg
	}
}

// Global EventBus instance
var GlobalEventBus = NewEventBus()
