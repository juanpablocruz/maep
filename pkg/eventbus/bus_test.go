package eventbus

import (
	"sync"
	"testing"
	"time"
)

type TestEvent struct {
	Type string
	Data string
}

func (e *TestEvent) GetType() string {
	return e.Type
}

type TestSubscriber struct {
	ch     chan Event
	wg     *sync.WaitGroup
	events []Event
	mu     sync.Mutex
}

func NewTestSubscriber() *TestSubscriber {
	return &TestSubscriber{
		ch:     make(chan Event, 10),
		wg:     &sync.WaitGroup{},
		events: []Event{},
	}
}

func (s *TestSubscriber) GetChannel() chan Event {
	return s.ch
}

func (s *TestSubscriber) GetWaitGroup() *sync.WaitGroup {
	return s.wg
}

func (s *TestSubscriber) OnEvent(event Event) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, event)
}

func (s *TestSubscriber) GetEvents() []Event {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]Event{}, s.events...)
}

func TestEventBus_Basic(t *testing.T) {
	bus := NewEventBus()

	subscriber := NewTestSubscriber()
	bus.Subscribe(subscriber)

	// Start the bus
	bus.Start()

	// Publish an event
	event := &TestEvent{Type: "test", Data: "hello"}
	bus.Publish(event)

	// Wait for processing
	subscriber.GetWaitGroup().Wait()

	// Check that the event was received
	events := subscriber.GetEvents()
	if len(events) != 1 {
		t.Errorf("Expected 1 event, got %d", len(events))
	}

	if events[0].GetType() != "test" {
		t.Errorf("Expected event type 'test', got '%s'", events[0].GetType())
	}
}

func TestEventBus_MultipleSubscribers(t *testing.T) {
	bus := NewEventBus()

	sub1 := NewTestSubscriber()
	sub2 := NewTestSubscriber()

	bus.Subscribe(sub1)
	bus.Subscribe(sub2)

	// Start the bus
	bus.Start()

	// Publish an event
	event := &TestEvent{Type: "test", Data: "hello"}
	bus.Publish(event)

	// Wait for processing
	sub1.GetWaitGroup().Wait()
	sub2.GetWaitGroup().Wait()

	// Check that both subscribers received the event
	events1 := sub1.GetEvents()
	events2 := sub2.GetEvents()

	if len(events1) != 1 {
		t.Errorf("Expected 1 event in sub1, got %d", len(events1))
	}

	if len(events2) != 1 {
		t.Errorf("Expected 1 event in sub2, got %d", len(events2))
	}
}

func TestEventBus_NoDeadlock(t *testing.T) {
	bus := NewEventBus()

	subscriber := NewTestSubscriber()
	bus.Subscribe(subscriber)

	// Start the bus
	bus.Start()

	// Publish multiple events rapidly
	for i := 0; i < 10; i++ {
		event := &TestEvent{Type: "test", Data: "hello"}
		bus.Publish(event)
	}

	// Wait for processing with timeout
	done := make(chan bool)
	go func() {
		subscriber.GetWaitGroup().Wait()
		done <- true
	}()

	select {
	case <-done:
		// Success
	case <-time.After(5 * time.Second):
		t.Fatal("Timeout waiting for event processing - possible deadlock")
	}

	// Check that all events were received
	events := subscriber.GetEvents()
	if len(events) != 10 {
		t.Errorf("Expected 10 events, got %d", len(events))
	}
}

func TestEventBus_PublishBeforeStart(t *testing.T) {
	bus := NewEventBus()

	subscriber := NewTestSubscriber()
	bus.Subscribe(subscriber)

	// Publish an event before starting the bus
	event := &TestEvent{Type: "test", Data: "hello"}
	bus.Publish(event)

	// Start the bus
	bus.Start()

	// Wait a bit to see if any events are processed
	time.Sleep(100 * time.Millisecond)

	// Check that no events were received (they should be lost)
	events := subscriber.GetEvents()
	if len(events) != 0 {
		t.Errorf("Expected 0 events (lost), got %d", len(events))
	}
}
