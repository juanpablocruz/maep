package fsm

import "github.com/juanpablocruz/maep/pkg/timer"

// IStateEvent represents an event for the initiator side of the FSM
type IStateEvent struct {
	Type InitiatorEventType
	Data any
}

// RStateEvent represents an event for the responder side of the FSM
type RStateEvent struct {
	Type ResponderEventType
	Data any
}

// InitiatorEventType represents the specific events that can trigger initiator state transitions
type InitiatorEventType int

const (
	IStartSession InitiatorEventType = iota
	IConnOK
	ISummaryResp
	ITimeout
	IAck
	IEndsyncRx
)

// ResponderEventType represents the specific events that can trigger responder state transitions
type ResponderEventType int

const (
	RIncomingSession ResponderEventType = iota
	RParamsOK
	RSummaryReq
	RChildrenReq
	RLeavesReq
	RDeltaChunk
	REndsyncTx
)

// GetType returns the event type name for IStateEvent
func (e *IStateEvent) GetType() string {
	return "InitiatorStateEvent"
}

// GetType returns the event type name for RStateEvent
func (e *RStateEvent) GetType() string {
	return "ResponderStateEvent"
}

// SendSummaryEvent represents an event for sending summary data
type SendSummaryEvent struct {
	Summary []byte
}

// GetType returns the event type name for SendSummaryEvent
func (e *SendSummaryEvent) GetType() string {
	return "SendSummaryEvent"
}

// SummaryResponseEvent represents a response to a summary request
type SummaryResponseEvent struct {
	RootEq bool // true if roots are equal, false if different
	Data   any
}

// GetType returns the event type name for SummaryResponseEvent
func (e *SummaryResponseEvent) GetType() string {
	return "SummaryResponseEvent"
}

// AckEvent represents an acknowledgment event
type AckEvent struct {
	HSeq       int  // highest sequence number acknowledged
	MoreChunks bool // whether there are more chunks to send
}

// GetType returns the event type name for AckEvent
func (e *AckEvent) GetType() string {
	return "AckEvent"
}

// Re-export timer events from the timer package
type TimerEvent = timer.TimerEvent

// DeltaChunkEvent represents a delta chunk event
type DeltaChunkEvent struct {
	Seq  int  // sequence number
	Last bool // whether this is the last chunk
	Data []byte
}

// GetType returns the event type name for DeltaChunkEvent
func (e *DeltaChunkEvent) GetType() string {
	return "DeltaChunkEvent"
}

// ChildrenReqEvent represents a children request event
type ChildrenReqEvent struct {
	Data any
}

// GetType returns the event type name for ChildrenReqEvent
func (e *ChildrenReqEvent) GetType() string {
	return "ChildrenReqEvent"
}

// LeavesReqEvent represents a leaves request event
type LeavesReqEvent struct {
	Data any
}

// GetType returns the event type name for LeavesReqEvent
func (e *LeavesReqEvent) GetType() string {
	return "LeavesReqEvent"
}

// =============================================================================
// Action Events - Events emitted by FSM for actions to be handled by subscribers
// =============================================================================

// Re-export timer events from the timer package
type ArmTimerEvent = timer.ArmTimerEvent
type DisarmTimerEvent = timer.DisarmTimerEvent

// RetransmitEvent represents the action to retransmit data
type RetransmitEvent struct {
	DataType string
	Data     any
}

// GetType returns the event type name for RetransmitEvent
func (e *RetransmitEvent) GetType() string {
	return "RetransmitEvent"
}

// SendChunkEvent represents the action to send a chunk of data
type SendChunkEvent struct {
	ChunkData []byte
	Seq       int
	Last      bool
}

// GetType returns the event type name for SendChunkEvent
func (e *SendChunkEvent) GetType() string {
	return "SendChunkEvent"
}

// CloseSessionEvent represents the action to close a session
type CloseSessionEvent struct {
	SessionID string
}

// GetType returns the event type name for CloseSessionEvent
func (e *CloseSessionEvent) GetType() string {
	return "CloseSessionEvent"
}

// ReplySummaryEvent represents the action to reply with summary data
type ReplySummaryEvent struct {
	Summary []byte
	RootEq  bool
}

// GetType returns the event type name for ReplySummaryEvent
func (e *ReplySummaryEvent) GetType() string {
	return "ReplySummaryEvent"
}

// ReplyChildrenEvent represents the action to reply with children data
type ReplyChildrenEvent struct {
	Children []byte
}

// GetType returns the event type name for ReplyChildrenEvent
func (e *ReplyChildrenEvent) GetType() string {
	return "ReplyChildrenEvent"
}

// ReplyLeavesEvent represents the action to reply with leaves data
type ReplyLeavesEvent struct {
	Leaves []byte
}

// GetType returns the event type name for ReplyLeavesEvent
func (e *ReplyLeavesEvent) GetType() string {
	return "ReplyLeavesEvent"
}

// StageChunkEvent represents the action to stage a chunk for processing
type StageChunkEvent struct {
	ChunkData []byte
	Seq       int
	Last      bool
}

// GetType returns the event type name for StageChunkEvent
func (e *StageChunkEvent) GetType() string {
	return "StageChunkEvent"
}

// ApplyDeterministicEvent represents the action to apply operations deterministically
type ApplyDeterministicEvent struct {
	Operations []byte
}

// GetType returns the event type name for ApplyDeterministicEvent
func (e *ApplyDeterministicEvent) GetType() string {
	return "ApplyDeterministicEvent"
}

// SendAckEvent represents the action to send an acknowledgment
type SendAckEvent struct {
	HSeq int
}

// GetType returns the event type name for SendAckEvent
func (e *SendAckEvent) GetType() string {
	return "SendAckEvent"
}

// EndsyncTxEvent represents the action to send endsync transaction
type EndsyncTxEvent struct {
	SessionID string
}

// GetType returns the event type name for EndsyncTxEvent
func (e *EndsyncTxEvent) GetType() string {
	return "EndsyncTxEvent"
}

// DescendDiffEvent represents the action to perform diff descent
type DescendDiffEvent struct {
	Summary []byte
}

// GetType returns the event type name for DescendDiffEvent
func (e *DescendDiffEvent) GetType() string {
	return "DescendDiffEvent"
}

// PlanDeltasEvent represents the action to plan delta operations
type PlanDeltasEvent struct {
	Diffs []byte
}

// GetType returns the event type name for PlanDeltasEvent
func (e *PlanDeltasEvent) GetType() string {
	return "PlanDeltasEvent"
}

// PrepareSVFenceEvent represents the action to prepare per-leaf SV fence
type PrepareSVFenceEvent struct {
	Leaves []byte
}

// GetType returns the event type name for PrepareSVFenceEvent
func (e *PrepareSVFenceEvent) GetType() string {
	return "PrepareSVFenceEvent"
}
