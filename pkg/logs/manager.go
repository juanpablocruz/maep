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
	mu         sync.RWMutex // Protect started field
	wg         sync.WaitGroup
}

func NewLogManager(logger *log.Logger) *LogManager {
	return &LogManager{
		subscriber: NewLogSubscriber(logger),
		ch:         make(chan eventbus.Event),
	}
}

func (lm *LogManager) Start(bus *eventbus.EventBus) {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	if lm.started {
		return
	}

	go func() {
		for event := range lm.ch {
			lm.subscriber.OnEvent(event)
			lm.wg.Done()
		}
	}()
	s := eventbus.NewSubscriber(lm.ch, &lm.wg)
	bus.Subscribe(*s)
	lm.started = true
}

func (lm *LogManager) WaitForProcessing() {
	lm.wg.Wait()
}

// GlobalLogManager instance
var GlobalLogManager = NewLogManager(log.Default())
