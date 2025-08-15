package timer

import (
	"sync"
	"testing"
	"time"

	"github.com/juanpablocruz/maep/pkg/eventbus"
)

// testSubscriber implements the eventbus.Subscriber interface for testing
type testSubscriber struct {
	eventChan chan eventbus.Event
	waitGroup *sync.WaitGroup
	onEvent   func(eventbus.Event)
}

func (t *testSubscriber) OnEvent(event eventbus.Event) {
	if t.onEvent != nil {
		t.onEvent(event)
	}
}

func (t *testSubscriber) GetChannel() chan eventbus.Event {
	return t.eventChan
}

func (t *testSubscriber) GetWaitGroup() *sync.WaitGroup {
	return t.waitGroup
}

// Test_TIMER_INIT_01_TimerSubscriberCreation validates timer subscriber creation
func Test_TIMER_INIT_01_TimerSubscriberCreation(t *testing.T) {
	// ID: TIMER-INIT-01
	// Target: TimerSubscriber
	// Setup: Create timer subscriber with event bus
	// Stimulus: Create new timer subscriber
	// Checks: Timer subscriber is created with correct initial state

	bus := eventbus.NewEventBus()
	timerSub := NewTimerSubscriber(bus)

	if timerSub == nil {
		t.Fatal("TimerSubscriber should not be nil")
	}

	if timerSub.bus != bus {
		t.Error("TimerSubscriber should have the correct event bus")
	}

	if len(timerSub.GetActiveTimers()) != 0 {
		t.Error("New timer subscriber should have no active timers")
	}
}

// Test_TIMER_ARM_01_ArmTimer validates timer arming functionality
func Test_TIMER_ARM_01_ArmTimer(t *testing.T) {
	// ID: TIMER-ARM-01
	// Target: TimerSubscriber
	// Setup: Create timer subscriber and start it
	// Stimulus: Arm a timer
	// Checks: Timer is armed and active

	bus := eventbus.NewEventBus()
	timerSub := NewTimerSubscriber(bus)

	// Start the event bus and timer subscriber
	bus.Start()
	timerSub.Start()

	// Arm a timer with a very long duration to prevent expiration during test
	bus.Publish(&ArmTimerEvent{
		TimerType: "test_timer",
		Duration:  60000, // 60 seconds
	})

	// Wait for timer to be armed
	time.Sleep(50 * time.Millisecond)

	// Check that timer is active
	activeTimers := timerSub.GetActiveTimers()
	if len(activeTimers) != 1 {
		t.Errorf("Expected 1 active timer, got %d", len(activeTimers))
	}

	if activeTimers[0] != "test_timer" {
		t.Errorf("Expected active timer 'test_timer', got '%s'", activeTimers[0])
	}

	// Disarm the timer before stopping to avoid hanging
	bus.Publish(&DisarmTimerEvent{
		TimerType: "test_timer",
	})

	// Wait a bit for the disarm to take effect
	time.Sleep(10 * time.Millisecond)

	// Clean up
	timerSub.Stop()
	bus.Stop()
}

// Test_TIMER_DISARM_01_DisarmTimer validates timer disarming functionality
func Test_TIMER_DISARM_01_DisarmTimer(t *testing.T) {
	// ID: TIMER-DISARM-01
	// Target: TimerSubscriber
	// Setup: Create timer subscriber with armed timer
	// Stimulus: Disarm the timer
	// Checks: Timer is disarmed and no longer active

	bus := eventbus.NewEventBus()
	timerSub := NewTimerSubscriber(bus)

	// Start the event bus and timer subscriber
	bus.Start()
	timerSub.Start()

	// Arm a timer
	bus.Publish(&ArmTimerEvent{
		TimerType: "test_timer",
		Duration:  1000, // 1 second
	})

	// Wait for timer to be armed
	time.Sleep(10 * time.Millisecond)

	// Verify timer is active
	if len(timerSub.GetActiveTimers()) != 1 {
		t.Error("Timer should be active after arming")
	}

	// Disarm the timer
	bus.Publish(&DisarmTimerEvent{
		TimerType: "test_timer",
	})

	// Wait for timer to be disarmed
	time.Sleep(10 * time.Millisecond)

	// Check that timer is no longer active
	activeTimers := timerSub.GetActiveTimers()
	if len(activeTimers) != 0 {
		t.Errorf("Expected 0 active timers after disarming, got %d", len(activeTimers))
	}

	// Clean up
	timerSub.Stop()
	bus.Stop()
}

// Test_TIMER_TIMEOUT_01_TimerTimeout validates timer timeout functionality
func Test_TIMER_TIMEOUT_01_TimerTimeout(t *testing.T) {
	// ID: TIMER-TIMEOUT-01
	// Target: TimerSubscriber
	// Setup: Create timer subscriber and subscribe to timeout events
	// Stimulus: Arm a timer and wait for timeout
	// Checks: Timeout event is published when timer expires

	bus := eventbus.NewEventBus()
	timerSub := NewTimerSubscriber(bus)

	// Track timeout events
	timeoutReceived := false
	var timeoutMutex sync.Mutex

	// Create a subscriber for timeout events
	timeoutSubscriber := &testSubscriber{
		eventChan: make(chan eventbus.Event, 1),
		waitGroup: &sync.WaitGroup{},
		onEvent: func(event eventbus.Event) {
			if timerEvent, ok := event.(*TimerEvent); ok {
				if timerEvent.TimerType == "test_timer" {
					timeoutMutex.Lock()
					timeoutReceived = true
					timeoutMutex.Unlock()
				}
			}
		},
	}

	// Subscribe to timeout events
	bus.Subscribe(timeoutSubscriber)

	// Start the event bus and timer subscriber
	bus.Start()
	timerSub.Start()

	// Arm a timer with short duration
	bus.Publish(&ArmTimerEvent{
		TimerType: "test_timer",
		Duration:  100, // 100ms
	})

	// Wait for timeout
	time.Sleep(200 * time.Millisecond)

	// Check that timeout event was received
	timeoutMutex.Lock()
	received := timeoutReceived
	timeoutMutex.Unlock()

	if !received {
		t.Error("Expected timeout event to be received")
	}

	// Check that timer is no longer active
	activeTimers := timerSub.GetActiveTimers()
	if len(activeTimers) != 0 {
		t.Errorf("Expected 0 active timers after timeout, got %d", len(activeTimers))
	}

	// Clean up
	timerSub.Stop()
	bus.Stop()
}

// Test_TIMER_MULTI_01_MultipleTimers validates handling of multiple timers
func Test_TIMER_MULTI_01_MultipleTimers(t *testing.T) {
	// ID: TIMER-MULTI-01
	// Target: TimerSubscriber
	// Setup: Create timer subscriber
	// Stimulus: Arm multiple timers with different types
	// Checks: Multiple timers can be managed independently

	bus := eventbus.NewEventBus()
	timerSub := NewTimerSubscriber(bus)

	// Start the event bus and timer subscriber
	bus.Start()
	timerSub.Start()

	// Arm multiple timers
	bus.Publish(&ArmTimerEvent{
		TimerType: "timer1",
		Duration:  1000,
	})

	bus.Publish(&ArmTimerEvent{
		TimerType: "timer2",
		Duration:  2000,
	})

	bus.Publish(&ArmTimerEvent{
		TimerType: "timer3",
		Duration:  3000,
	})

	// Wait for timers to be armed
	time.Sleep(10 * time.Millisecond)

	// Check that all timers are active
	activeTimers := timerSub.GetActiveTimers()
	if len(activeTimers) != 3 {
		t.Errorf("Expected 3 active timers, got %d", len(activeTimers))
	}

	// Disarm one timer
	bus.Publish(&DisarmTimerEvent{
		TimerType: "timer2",
	})

	// Wait for timer to be disarmed
	time.Sleep(10 * time.Millisecond)

	// Check that only 2 timers remain active
	activeTimers = timerSub.GetActiveTimers()
	if len(activeTimers) != 2 {
		t.Errorf("Expected 2 active timers after disarming one, got %d", len(activeTimers))
	}

	// Verify the remaining timers
	timer1Found := false
	timer3Found := false
	for _, timer := range activeTimers {
		if timer == "timer1" {
			timer1Found = true
		}
		if timer == "timer3" {
			timer3Found = true
		}
	}

	if !timer1Found {
		t.Error("Timer1 should still be active")
	}
	if !timer3Found {
		t.Error("Timer3 should still be active")
	}

	// Clean up
	timerSub.Stop()
	bus.Stop()
}

// Test_TIMER_REARM_01_ReArmTimer validates re-arming a timer
func Test_TIMER_REARM_01_ReArmTimer(t *testing.T) {
	// ID: TIMER-REARM-01
	// Target: TimerSubscriber
	// Setup: Create timer subscriber with armed timer
	// Stimulus: Re-arm the same timer type
	// Checks: Old timer is stopped and new timer is armed

	bus := eventbus.NewEventBus()
	timerSub := NewTimerSubscriber(bus)

	// Start the event bus and timer subscriber
	bus.Start()
	timerSub.Start()

	// Arm a timer
	bus.Publish(&ArmTimerEvent{
		TimerType: "test_timer",
		Duration:  1000,
	})

	// Wait for timer to be armed
	time.Sleep(10 * time.Millisecond)

	// Verify timer is active
	if len(timerSub.GetActiveTimers()) != 1 {
		t.Error("Timer should be active after arming")
	}

	// Re-arm the same timer with different duration
	bus.Publish(&ArmTimerEvent{
		TimerType: "test_timer",
		Duration:  2000,
	})

	// Wait for timer to be re-armed
	time.Sleep(10 * time.Millisecond)

	// Check that only one timer is active (the re-armed one)
	activeTimers := timerSub.GetActiveTimers()
	if len(activeTimers) != 1 {
		t.Errorf("Expected 1 active timer after re-arming, got %d", len(activeTimers))
	}

	if activeTimers[0] != "test_timer" {
		t.Errorf("Expected active timer 'test_timer', got '%s'", activeTimers[0])
	}

	// Clean up
	timerSub.Stop()
	bus.Stop()
}

// Test_TIMER_STOP_01_StopSubscriber validates stopping the timer subscriber
func Test_TIMER_STOP_01_StopSubscriber(t *testing.T) {
	// ID: TIMER-STOP-01
	// Target: TimerSubscriber
	// Setup: Create timer subscriber with active timers
	// Stimulus: Stop the timer subscriber
	// Checks: All timers are disarmed and subscriber stops gracefully

	bus := eventbus.NewEventBus()
	timerSub := NewTimerSubscriber(bus)

	// Start the event bus and timer subscriber
	bus.Start()
	timerSub.Start()

	// Arm multiple timers
	bus.Publish(&ArmTimerEvent{
		TimerType: "timer1",
		Duration:  1000,
	})

	bus.Publish(&ArmTimerEvent{
		TimerType: "timer2",
		Duration:  2000,
	})

	// Wait for timers to be armed
	time.Sleep(10 * time.Millisecond)

	// Verify timers are active
	if len(timerSub.GetActiveTimers()) != 2 {
		t.Error("Timers should be active before stopping")
	}

	// Stop the timer subscriber
	timerSub.Stop()

	// Check that all timers are disarmed
	activeTimers := timerSub.GetActiveTimers()
	if len(activeTimers) != 0 {
		t.Errorf("Expected 0 active timers after stopping, got %d", len(activeTimers))
	}

	// Clean up
	bus.Stop()
}
