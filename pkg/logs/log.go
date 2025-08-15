// Package logs provides logging utilities.
package logs

import (
	"log"
	"sync"

	"github.com/juanpablocruz/maep/pkg/eventbus"
)

// LogSubscriber sink that receives events from the eventbus and pipes them to the log.
type LogSubscriber struct {
	log *log.Logger
	ch  chan eventbus.Event
	wg  *sync.WaitGroup
}

func NewLogSubscriber(log *log.Logger) *LogSubscriber {
	return &LogSubscriber{
		log: log,
		ch:  make(chan eventbus.Event, 100), // Buffered channel to prevent blocking
		wg:  &sync.WaitGroup{},
	}
}

func (s *LogSubscriber) GetChannel() chan eventbus.Event {
	return s.ch
}

func (s *LogSubscriber) GetWaitGroup() *sync.WaitGroup {
	return s.wg
}

// OnEvent handles any event and logs it
func (s *LogSubscriber) OnEvent(event eventbus.Event) {
	if l, ok := event.(*LogEvent); ok {
		s.log.Printf("[EVENT](%s), Event: %+v", l.Event.GetType(), l.Event)
	} else if i, ok := event.(*InstrumentationEvent); ok {
		// Log instrumentation events with more detail
		if i.Data != nil {
			s.log.Printf("[%s][%s]: %s - %+v", i.Level, i.Source, i.EventType, i.Data)
		} else {
			s.log.Printf("[%s][%s]: %s", i.Level, i.Source, i.EventType)
		}
	} else if p, ok := event.(*PerformanceEvent); ok {
		s.log.Printf("[PERF]: %s took %v (count: %d)", p.Operation, p.Duration, p.Count)
	} else if st, ok := event.(*StateChangeEvent); ok {
		s.log.Printf("[STATE]: %s -> %s (trigger: %s)", st.FromState, st.ToState, st.Trigger)
	} else if err, ok := event.(*ErrorEvent); ok {
		s.log.Printf("[ERROR][%s]: %s - %v", err.Context, err.Operation, err.Error)
	} else {
		// Handle generic events
		s.log.Printf("[EVENT](%s), Event: %+v", event.GetType(), event)
	}
}
