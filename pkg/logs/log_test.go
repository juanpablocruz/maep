package logs

import (
	"bytes"
	"log"
	"strings"
	"testing"

	"github.com/juanpablocruz/maep/pkg/eventbus"
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
	s := NewLogSubscriber(logger)

	// Subscribe to the EventBus
	bus.Subscribe(s)

	// Start the EventBus
	bus.Start()

	// Create and publish different types of events
	opEvent := &SimpleEvent{Message: "Test Message"}
	t.Logf("Publishing SimpleEvent: %+v", opEvent)
	bus.Publish(opEvent)

	// Create a custom event
	customEvent := &CustomEvent{Message: "Hello World"}
	t.Logf("Publishing CustomEvent: %+v", customEvent)
	bus.Publish(customEvent)

	// Wait for all events to be processed before reading the buffer
	t.Logf("Waiting for events to be processed...")
	s.GetWaitGroup().Wait()
	t.Logf("Events processed, checking buffer...")

	// Check that both events were logged
	logOutput := buf.String()
	t.Logf("Log output: %s", logOutput)
	t.Logf("Log output length: %d", len(logOutput))

	if !strings.Contains(logOutput, "SimpleEvent") {
		t.Errorf("Expected log to contain SimpleEvent")
	}

	if !strings.Contains(logOutput, "CustomEvent") {
		t.Errorf("Expected log to contain CustomEvent")
	}

	if !strings.Contains(logOutput, "Hello World") {
		t.Errorf("Expected log to contain custom event message")
	}
}
