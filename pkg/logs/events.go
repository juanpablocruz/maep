package logs

import (
	"time"

	"github.com/juanpablocruz/maep/pkg/eventbus"
)

type Level string

const (
	LevelDebug Level = "DEBUG"
	LevelInfo  Level = "INFO"
	LevelWarn  Level = "WARN"
	LevelError Level = "ERROR"
)

// InstrumentationEvent represents a structured instrumentation event
type InstrumentationEvent struct {
	EventType string
	Level     Level
	Data      interface{}
	Timestamp time.Time
	Source    string
}

func (i *InstrumentationEvent) GetType() string {
	return "InstrumentationEvent"
}

// LogEvent wraps other events for logging
type LogEvent struct {
	EventType string
	Event     eventbus.Event
	Timestamp time.Time
}

func (l *LogEvent) GetType() string {
	return "LogEvent"
}

// PerformanceEvent represents performance-related metrics
type PerformanceEvent struct {
	Operation string
	Duration  time.Duration
	Count     int64
	Data      interface{}
	Timestamp time.Time
}

func (p *PerformanceEvent) GetType() string {
	return "PerformanceEvent"
}

// StateChangeEvent represents state transitions
type StateChangeEvent struct {
	FromState string
	ToState   string
	Trigger   string
	Data      interface{}
	Timestamp time.Time
}

func (s *StateChangeEvent) GetType() string {
	return "StateChangeEvent"
}

// ErrorEvent represents error conditions
type ErrorEvent struct {
	Error     error
	Context   string
	Operation string
	Data      interface{}
	Timestamp time.Time
}

func (e *ErrorEvent) GetType() string {
	return "ErrorEvent"
}
