package logs

import (
	"bytes"
	"log"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/juanpablocruz/maep/pkg/eventbus"
)

// ThreadSafeBuffer is a thread-safe wrapper around bytes.Buffer
type ThreadSafeBuffer struct {
	buf bytes.Buffer
	mu  sync.RWMutex
}

func (tsb *ThreadSafeBuffer) Write(p []byte) (n int, err error) {
	tsb.mu.Lock()
	defer tsb.mu.Unlock()
	return tsb.buf.Write(p)
}

func (tsb *ThreadSafeBuffer) String() string {
	tsb.mu.RLock()
	defer tsb.mu.RUnlock()
	return tsb.buf.String()
}

func Test_LogManager_EventBus_Integration(t *testing.T) {
	// Create EventBus
	bus := eventbus.NewEventBus()

	// Create a thread-safe buffer to capture log output
	tsBuffer := &ThreadSafeBuffer{}
	logger := log.New(tsBuffer, "", 0)

	// Create LogManager and start it
	logManager := NewLogManager(logger)
	logManager.Start(bus)

	// Create and publish different types of events

	opEvent := &SimpleEvent{Message: "Test Message"}
	bus.Publish(opEvent)

	// Create a custom event
	customEvent := &CustomEvent{Message: "Hello World"}
	bus.Publish(customEvent)

	// Wait for both events to be processed
	logManager.WaitForProcessing()

	// Add a small delay to ensure all events are processed
	// This is needed because the EventBus publishes asynchronously
	// and there might be a small timing issue
	time.Sleep(10 * time.Millisecond)

	// Check that both events were logged
	logOutput := tsBuffer.String()

	t.Logf("Log output: %s", logOutput)
	t.Logf("Log output lines: %d", strings.Count(logOutput, "\n"))

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
