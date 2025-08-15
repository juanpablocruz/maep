package timer

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/juanpablocruz/maep/pkg/eventbus"
)

// TimerSubscriber handles timer events and manages actual timers
type TimerSubscriber struct {
	bus       *eventbus.EventBus
	timers    map[string]*time.Timer
	timerMux  sync.RWMutex
	ctx       context.Context
	cancel    context.CancelFunc
	waitGroup *sync.WaitGroup
}

// NewTimerSubscriber creates a new timer subscriber
func NewTimerSubscriber(bus *eventbus.EventBus) *TimerSubscriber {
	ctx, cancel := context.WithCancel(context.Background())
	return &TimerSubscriber{
		bus:       bus,
		timers:    make(map[string]*time.Timer),
		ctx:       ctx,
		cancel:    cancel,
		waitGroup: &sync.WaitGroup{},
	}
}

// Start begins listening for timer events
func (ts *TimerSubscriber) Start() {
	// Subscribe to timer events before starting
	subscriber := &timerEventSubscriber{
		timerSub:  ts,
		eventChan: make(chan eventbus.Event, 10),
		waitGroup: ts.waitGroup,
	}

	ts.bus.Subscribe(subscriber)

	ts.waitGroup.Add(1)
	go ts.listenForEvents(subscriber)
}

// Stop stops the timer subscriber and cancels all timers
func (ts *TimerSubscriber) Stop() {
	ts.cancel()
	ts.disarmAllTimers()
	// Don't wait for the goroutine to finish - context cancellation should be sufficient
}

// listenForEvents listens for timer-related events
func (ts *TimerSubscriber) listenForEvents(subscriber *timerEventSubscriber) {
	defer ts.waitGroup.Done()

	// Listen for events until context is cancelled
	for {
		select {
		case event := <-subscriber.eventChan:
			ts.handleEvent(event)
		case <-ts.ctx.Done():
			// Don't close the channel - the event bus will handle that
			return
		}
	}
}

// handleEvent processes timer events
func (ts *TimerSubscriber) handleEvent(event eventbus.Event) {
	switch e := event.(type) {
	case *ArmTimerEvent:
		ts.armTimer(e.TimerType, e.Duration)
	case *DisarmTimerEvent:
		ts.disarmTimer(e.TimerType)
	default:
		// Ignore other events
	}
}

// armTimer arms a timer with the specified type and duration
func (ts *TimerSubscriber) armTimer(timerType string, durationMs int64) {
	ts.timerMux.Lock()
	defer ts.timerMux.Unlock()

	// Disarm existing timer if it exists
	if existingTimer, exists := ts.timers[timerType]; exists {
		existingTimer.Stop()
	}

	// Create new timer
	duration := time.Duration(durationMs) * time.Millisecond
	timer := time.AfterFunc(duration, func() {
		ts.handleTimeout(timerType)
	})

	ts.timers[timerType] = timer
	fmt.Printf("Timer armed: %s for %v\n", timerType, duration)
}

// disarmTimer disarms a timer with the specified type
func (ts *TimerSubscriber) disarmTimer(timerType string) {
	ts.timerMux.Lock()
	defer ts.timerMux.Unlock()

	if timer, exists := ts.timers[timerType]; exists {
		timer.Stop()
		delete(ts.timers, timerType)
		fmt.Printf("Timer disarmed: %s\n", timerType)
	}
}

// disarmAllTimers disarms all active timers
func (ts *TimerSubscriber) disarmAllTimers() {
	ts.timerMux.Lock()
	defer ts.timerMux.Unlock()

	for timerType, timer := range ts.timers {
		timer.Stop()
		fmt.Printf("Timer disarmed: %s\n", timerType)
	}
	ts.timers = make(map[string]*time.Timer)
}

// handleTimeout handles a timer timeout
func (ts *TimerSubscriber) handleTimeout(timerType string) {
	// Remove the timer from the map
	ts.timerMux.Lock()
	delete(ts.timers, timerType)
	ts.timerMux.Unlock()

	// Publish timeout event
	timeoutEvent := &TimerEvent{
		TimerType: timerType,
	}
	ts.bus.Publish(timeoutEvent)
	fmt.Printf("Timer timeout: %s\n", timerType)
}

// GetActiveTimers returns a list of active timer types (for debugging/testing)
func (ts *TimerSubscriber) GetActiveTimers() []string {
	ts.timerMux.RLock()
	defer ts.timerMux.RUnlock()

	timers := make([]string, 0, len(ts.timers))
	for timerType := range ts.timers {
		timers = append(timers, timerType)
	}
	return timers
}

// timerEventSubscriber implements the eventbus.Subscriber interface for timer events
type timerEventSubscriber struct {
	timerSub  *TimerSubscriber
	eventChan chan eventbus.Event
	waitGroup *sync.WaitGroup
}

func NewTimerEventSubscriber(bus *eventbus.EventBus) *timerEventSubscriber {
	return &timerEventSubscriber{
		timerSub:  NewTimerSubscriber(bus),
		eventChan: make(chan eventbus.Event, 10),
		waitGroup: &sync.WaitGroup{},
	}
}

func (tes *timerEventSubscriber) OnEvent(event eventbus.Event) {
	// Only handle timer-related events
	switch event.(type) {
	case *ArmTimerEvent, *DisarmTimerEvent:
		tes.eventChan <- event
	}
}

func (tes *timerEventSubscriber) GetChannel() chan eventbus.Event {
	return tes.eventChan
}

func (tes *timerEventSubscriber) GetWaitGroup() *sync.WaitGroup {
	return tes.waitGroup
}
