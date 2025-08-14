package logs

import "github.com/juanpablocruz/maep/pkg/eventbus"

type LogEvent struct {
	EventType string
	Event     eventbus.Event
}

func (l *LogEvent) GetType() string {
	return l.EventType
}
