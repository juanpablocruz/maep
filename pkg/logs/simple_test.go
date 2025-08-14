package logs

import (
	"bytes"
	"log"
	"strings"
	"testing"
)

// SimpleEvent for testing
type SimpleEvent struct {
	Message string
}

func (s *SimpleEvent) GetType() string {
	return "SimpleEvent"
}

func Test_LogSubscriber_Simple(t *testing.T) {
	// Create a buffer to capture log output
	var buf bytes.Buffer
	logger := log.New(&buf, "", 0)

	// Create LogSubscriber
	subscriber := NewLogSubscriber(logger)

	// Test with a simple event
	event := &SimpleEvent{Message: "Test Message"}

	// Call OnEvent directly
	subscriber.OnEvent(event)

	// Check the output
	logOutput := buf.String()
	t.Logf("Log output: %s", logOutput)

	if !strings.Contains(logOutput, "SimpleEvent") {
		t.Errorf("Expected log to contain SimpleEvent")
	}

	if !strings.Contains(logOutput, "Test Message") {
		t.Errorf("Expected log to contain Test Message")
	}
}
