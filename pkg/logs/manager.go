package logs

import (
	"log"
	"sync"

	"github.com/juanpablocruz/maep/pkg/eventbus"
)

type LogManager struct {
	subscriber *LogSubscriber
	started    bool
	mu         sync.RWMutex // Protect started field
}

func NewLogManager(logger *log.Logger) *LogManager {
	return &LogManager{
		subscriber: NewLogSubscriber(logger),
	}
}

func (lm *LogManager) Start(bus *eventbus.EventBus) {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	if lm.started {
		return
	}

	bus.Subscribe(lm.subscriber)
	bus.Start()
	lm.started = true
}

func (lm *LogManager) WaitForProcessing() {
	lm.subscriber.GetWaitGroup().Wait()
}

// GlobalLogManager instance
var GlobalLogManager = NewLogManager(log.Default())
