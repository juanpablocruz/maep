package node

import "time"

type EventType string

const (
	EventPut          EventType = "put"
	EventDel          EventType = "del"
	EventSendSummary  EventType = "send_summary"
	EventSendReq      EventType = "send_req"
	EventSendDelta    EventType = "send_delta"
	EventAppliedDelta EventType = "applied_delta"
	EventWarn         EventType = "warn"
	EventConnChange   EventType = "conn_change"
	EventPauseChange  EventType = "pause_change"

	EventHB             EventType = "hb"
	EventHealth         EventType = "health"
	EventSync           EventType = "sync"
	EventSendSegAd      EventType = "send_segad"
	EventSendSegKeysReq EventType = "send_segkeys_req"
	EventSendSegKeys    EventType = "send_segkeys"
	EventWire           EventType = "wire"
	EventSendDeltaChunk EventType = "send_delta_chunk"
	EventRecvDeltaChunk EventType = "recv_delta_chunk"
	EventAck            EventType = "ack"
)

type Event struct {
	Time   time.Time
	Node   string
	Type   EventType
	Fields map[string]any
}
