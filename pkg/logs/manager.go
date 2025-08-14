package logs

import (
	"log"
	"sync"

	"github.com/juanpablocruz/maep/pkg/eventbus"
)

type LogManager struct {
	subscriber *LogSubscriber
	ch         chan eventbus.Event
	started    bool
	wg         sync.WaitGroup
}

func NewLogManager(logger *log.Logger) *LogManager {
	return &LogManager{
		subscriber: NewLogSubscriber(logger),
		ch:         make(chan eventbus.Event),
	}
}

func (lm *LogManager) Start(bus *eventbus.EventBus) {
	if lm.started {
		return
	}

	go func() {
		for event := range lm.ch {
			lm.wg.Add(1)
			lm.subscriber.OnEvent(event)
			lm.wg.Done()
		}
	}()
	bus.Subscribe(lm.ch)
	lm.started = true
}

func (lm *LogManager) WaitForProcessing() {
	lm.wg.Wait()
}

// GlobalLogManager instance
var GlobalLogManager = NewLogManager(log.Default())
