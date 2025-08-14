// Package logs provides logging utilities.
package logs

import (
	"log"

	"github.com/juanpablocruz/maep/pkg/eventbus"
)

// LogSubscriber sink that receives events from the eventbus and pipes them to the log.
type LogSubscriber struct {
	log *log.Logger
}

func NewLogSubscriber(log *log.Logger) *LogSubscriber {
	return &LogSubscriber{log: log}
}

// OnEvent handles any event and logs it
func (s *LogSubscriber) OnEvent(event eventbus.Event) {
	s.log.Printf("[EVENT](%s), Event: %+v", event.GetType(), event)
}
