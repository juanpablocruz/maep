package logs

import (
	"bytes"
	"log"
	"strings"
	"sync"
	"testing"

	"github.com/juanpablocruz/maep/pkg/engine"
	"github.com/juanpablocruz/maep/pkg/eventbus"
	"github.com/juanpablocruz/maep/pkg/testutils"
)

type CustomEvent struct {
	Message string
}

func (c *CustomEvent) GetType() string {
	return "CustomEvent"
}

func Test_LogSubscriber_EventBus_Integration(t *testing.T) {
	// Create EventBus
	bus := eventbus.NewEventBus()

	// Create a buffer to capture log output
	var buf bytes.Buffer
	logger := log.New(&buf, "", 0)

	// Create LogSubscriber
	subscriber := NewLogSubscriber(logger)

	// Subscribe to the EventBus
	ch := make(chan eventbus.Event)
	wg := &sync.WaitGroup{}
	s := eventbus.NewSubscriber(ch, wg)
	go func() {
		for event := range ch {
			subscriber.OnEvent(event)
			wg.Done()
		}
	}()
	bus.Subscribe(*s)

	// Create and publish different types of events
	op := testutils.GenerateOp("test-key", "test-value", engine.OpPut)

	opEvent := &engine.OpEvent{Op: &op}
	bus.Publish(opEvent)

	// Create a custom event
	customEvent := &CustomEvent{Message: "Hello World"}
	bus.Publish(customEvent)

	// Wait for all events to be processed before reading the buffer
	wg.Wait()

	// Check that both events were logged
	logOutput := buf.String()
	t.Logf("Log output: %s", logOutput)

	if !strings.Contains(logOutput, "OpEvent") {
		t.Errorf("Expected log to contain OpEvent")
	}

	if !strings.Contains(logOutput, "CustomEvent") {
		t.Errorf("Expected log to contain CustomEvent")
	}

	if !strings.Contains(logOutput, "Hello World") {
		t.Errorf("Expected log to contain custom event message")
	}
}
