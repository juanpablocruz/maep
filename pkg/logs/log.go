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
		ch:  make(chan eventbus.Event),
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
	s.log.Printf("[EVENT](%s), Event: %+v", event.GetType(), event)
}
