package timer

// TimerEvent represents a timeout event
type TimerEvent struct {
	TimerType string
}

// GetType returns the event type name for TimerEvent
func (e *TimerEvent) GetType() string {
	return "TimerEvent"
}

// ArmTimerEvent represents the action to arm a timer
type ArmTimerEvent struct {
	TimerType string
	Duration  int64 // duration in milliseconds
}

// GetType returns the event type name for ArmTimerEvent
func (e *ArmTimerEvent) GetType() string {
	return "ArmTimerEvent"
}

// DisarmTimerEvent represents the action to disarm a timer
type DisarmTimerEvent struct {
	TimerType string
}

// GetType returns the event type name for DisarmTimerEvent
func (e *DisarmTimerEvent) GetType() string {
	return "DisarmTimerEvent"
}
