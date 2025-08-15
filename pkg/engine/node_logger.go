package engine

import (
	"time"

	"github.com/juanpablocruz/maep/pkg/eventbus"
	"github.com/juanpablocruz/maep/pkg/logs"
)

func (n *Node) Emit(eventType string, data eventbus.Event) {
	n.mu.RLock()
	started := n.started
	n.mu.RUnlock()

	if !started {
		return
	}

	n.bus.Publish(&logs.LogEvent{
		EventType: eventType,
		Event:     data,
		Timestamp: time.Now(),
	})
}

func (n *Node) EmitInstrumentation(eventType string, data any) {
	n.mu.RLock()
	started := n.started
	n.mu.RUnlock()

	if !started {
		return
	}

	n.bus.Publish(&logs.InstrumentationEvent{
		EventType: eventType,
		Level:     logs.LevelInfo,
		Data:      data,
		Timestamp: time.Now(),
		Source:    "Node",
	})
}

// EmitPerformance emits performance-related events
func (n *Node) EmitPerformance(operation string, duration time.Duration, count int64, data any) {
	n.mu.RLock()
	started := n.started
	n.mu.RUnlock()

	if !started {
		return
	}

	n.bus.Publish(&logs.PerformanceEvent{
		Operation: operation,
		Duration:  duration,
		Count:     count,
		Data:      data,
		Timestamp: time.Now(),
	})
}

// EmitStateChange emits state transition events
func (n *Node) EmitStateChange(fromState, toState, trigger string, data any) {
	n.mu.RLock()
	started := n.started
	n.mu.RUnlock()

	if !started {
		return
	}

	n.bus.Publish(&logs.StateChangeEvent{
		FromState: fromState,
		ToState:   toState,
		Trigger:   trigger,
		Data:      data,
		Timestamp: time.Now(),
	})
}

// EmitError emits error events
func (n *Node) EmitError(err error, context, operation string, data any) {
	n.mu.RLock()
	started := n.started
	n.mu.RUnlock()

	if !started {
		return
	}

	n.bus.Publish(&logs.ErrorEvent{
		Error:     err,
		Context:   context,
		Operation: operation,
		Data:      data,
		Timestamp: time.Now(),
	})
}
