// Package fsm implements a finite state machine in charge of managing the node's state machine.
package fsm

import "github.com/juanpablocruz/maep/pkg/eventbus"

// States:
//
// Initiator:
// IDLE
// SESSION_OPEN
// SEND_SUMMARY
// WAIT_PROOFS
// COMPUTE_MISSING
// SEND_DELTA
// WAIT_ACK
// NO_MORE
// COMMIT_CLOSE
//
// Responder:
// IDLE
// SESSION_ACCEPT
// RECV_SUMMARY
// DIFF_DESCENT
// APPLY_PROOFS
// RECV_DELTA
// APPLY_OPS
// SEND_ACK
// CHECK_DIFFS
// COMMIT_CLOSE
//
//
// Every node has two states, one models its relation with the successor node, and the other with its predecessor.
//

type InitiatorState int

const (
	IIdle InitiatorState = iota
	ISessionOpen
	ISendSummary
	IWaitProofs
	IComputeMissing
	ISendDelta
	IWaitAck
	INoMore
	ICommitClose
)

// ResponderState represents the possible states for the responder side of the FSM
type ResponderState int

const (
	RIdle ResponderState = iota
	RSessionAccept
	RRecvSummary
	RDiffDescent
	RApplyProofs
	RRecvDelta
	RApplyOps
	RSendAck
	RCheckDiffs
	RCommitClose
)

// FSM represents the finite state machine managing node state transitions
type FSM struct {
	initiatorState FSMInitiatorState
	responderState FSMResponderState
	bus            *eventbus.EventBus
	// State variables for guards
	diffsRemain bool
	moreChunks  bool
}

// FSMInitiatorState defines the interface for initiator state implementations
type FSMInitiatorState interface {
	Enter(ev *IStateEvent)
	Transition(ev *IStateEvent) FSMInitiatorState
	GetState() InitiatorState
}

// FSMResponderState defines the interface for responder state implementations
type FSMResponderState interface {
	Enter(ev *RStateEvent)
	Transition(ev *RStateEvent) FSMResponderState
	GetState() ResponderState
}

// HandleInitiatorEvent processes an event for the initiator side of the FSM
func (f *FSM) HandleInitiatorEvent(ev *IStateEvent) {
	nextState := f.initiatorState.Transition(ev)
	if nextState != nil && nextState != f.initiatorState {
		f.initiatorState = nextState
		f.initiatorState.Enter(ev)
	} else {
		// Handle "on enter" transitions that happen within the Enter method
		f.initiatorState.Enter(ev)
	}
}

// HandleResponderEvent processes an event for the responder side of the FSM
func (f *FSM) HandleResponderEvent(ev *RStateEvent) {
	nextState := f.responderState.Transition(ev)
	if nextState != nil && nextState != f.responderState {
		f.responderState = nextState
		f.responderState.Enter(ev)
	} else {
		// Handle "on enter" transitions that happen within the Enter method
		f.responderState.Enter(ev)
	}
}

// NewFSM creates a new FSM instance with default states
func NewFSM(bus *eventbus.EventBus) *FSM {
	fsm := &FSM{
		bus:         bus,
		diffsRemain: false,
		moreChunks:  false,
	}
	fsm.initiatorState = &InitiatorIdleState{fsm: fsm}
	fsm.responderState = &ResponderIdleState{fsm: fsm}
	return fsm
}

// GetInitiatorState returns the current initiator state
func (f *FSM) GetInitiatorState() InitiatorState {
	return f.initiatorState.GetState()
}

// GetResponderState returns the current responder state
func (f *FSM) GetResponderState() ResponderState {
	return f.responderState.GetState()
}

// SetDiffsRemain sets the diffsRemain guard variable
func (f *FSM) SetDiffsRemain(remain bool) {
	f.diffsRemain = remain
}

// SetMoreChunks sets the moreChunks guard variable
func (f *FSM) SetMoreChunks(more bool) {
	f.moreChunks = more
}

// =============================================================================
// Initiator State Implementations (following the transition table exactly)
// =============================================================================

// InitiatorIdleState represents the idle state for the initiator
type InitiatorIdleState struct {
	fsm *FSM
}

func (s *InitiatorIdleState) Enter(ev *IStateEvent) {
	// No action needed in idle state
}

func (s *InitiatorIdleState) Transition(ev *IStateEvent) FSMInitiatorState {
	switch ev.Type {
	case IStartSession:
		next := &InitiatorSessionOpenState{fsm: s.fsm}
		next.Enter(ev)
		return next
	default:
		return s // Stay in current state for unhandled events
	}
}

func (s *InitiatorIdleState) GetState() InitiatorState {
	return IIdle
}

// InitiatorSessionOpenState represents the session open state for the initiator
type InitiatorSessionOpenState struct {
	fsm *FSM
}

func (s *InitiatorSessionOpenState) Enter(ev *IStateEvent) {
	// Session is now open, waiting for connection OK
}

func (s *InitiatorSessionOpenState) Transition(ev *IStateEvent) FSMInitiatorState {
	switch ev.Type {
	case IConnOK:
		next := &InitiatorSendSummaryState{fsm: s.fsm}
		next.Enter(ev)
		return next
	default:
		return s
	}
}

func (s *InitiatorSessionOpenState) GetState() InitiatorState {
	return ISessionOpen
}

// InitiatorSendSummaryState handles sending summary to responder
type InitiatorSendSummaryState struct {
	fsm *FSM
}

func (s *InitiatorSendSummaryState) Enter(ev *IStateEvent) {
	// SendSummary(); ArmTimer()
	if summary, ok := ev.Data.([]byte); ok {
		s.fsm.bus.Publish(&SendSummaryEvent{Summary: summary})
	}
	// ArmTimer() would be implemented here
	s.fsm.bus.Publish(&ArmTimerEvent{
		TimerType: "summary_timeout",
		Duration:  5000, // 5 seconds
	})

	// On enter, immediately transition to WAIT_PROOFS
	nextState := &InitiatorWaitProofsState{fsm: s.fsm}
	s.fsm.initiatorState = nextState
	// Call Enter on the next state to complete the transition
	nextState.Enter(ev)
}

func (s *InitiatorSendSummaryState) Transition(ev *IStateEvent) FSMInitiatorState {
	// This state immediately transitions on enter, so no external events are handled
	return s
}

func (s *InitiatorSendSummaryState) GetState() InitiatorState {
	return ISendSummary
}

// InitiatorWaitProofsState waits for proofs from responder
type InitiatorWaitProofsState struct {
	fsm *FSM
}

func (s *InitiatorWaitProofsState) Enter(ev *IStateEvent) {
	// Waiting for proofs to arrive
}

func (s *InitiatorWaitProofsState) Transition(ev *IStateEvent) FSMInitiatorState {
	switch ev.Type {
	case ISummaryResp:
		// Check guard: rootEq
		if summaryResp, ok := ev.Data.(*SummaryResponseEvent); ok {
			// DisarmTimer() action
			s.fsm.bus.Publish(&DisarmTimerEvent{TimerType: "summary_timeout"})

			if summaryResp.RootEq {
				// rootEq: transition to NO_MORE
				next := &InitiatorNoMoreState{fsm: s.fsm}
				next.Enter(ev)
				return next
			} else {
				// rootNe: transition to COMPUTE_MISSING
				next := &InitiatorComputeMissingState{fsm: s.fsm}
				next.Enter(ev)
				return next
			}
		}
		return s
	case ITimeout:
		// Timeout: retransmit and go back to SEND_SUMMARY
		// Retransmit() action
		s.fsm.bus.Publish(&RetransmitEvent{
			DataType: "summary",
			Data:     ev.Data,
		})
		next := &InitiatorSendSummaryState{fsm: s.fsm}
		next.Enter(ev)
		return next
	default:
		return s
	}
}

func (s *InitiatorWaitProofsState) GetState() InitiatorState {
	return IWaitProofs
}

// InitiatorComputeMissingState computes missing operations
type InitiatorComputeMissingState struct {
	fsm *FSM
}

func (s *InitiatorComputeMissingState) Enter(ev *IStateEvent) {
	// DescendDiff(); if no leaves → NO_MORE; else PlanDeltas()
	// This would compute missing operations and set diffsRemain/moreChunks
	s.fsm.SetDiffsRemain(true) // Example: set based on actual computation
	s.fsm.SetMoreChunks(true)  // Example: set based on actual computation

	// DescendDiff() action
	// The data would come from the summary response, but for now we'll use placeholder data
	s.fsm.bus.Publish(&DescendDiffEvent{
		Summary: []byte("summary_data"),
	})

	// PlanDeltas() action (if diffs remain)
	if s.fsm.diffsRemain {
		s.fsm.bus.Publish(&PlanDeltasEvent{
			Diffs: []byte("computed_diffs"),
		})
	}

	// Check guards and transition accordingly
	if !s.fsm.diffsRemain {
		// No diffs remain: go to NO_MORE
		nextState := &InitiatorNoMoreState{fsm: s.fsm}
		s.fsm.initiatorState = nextState
		// Call Enter on the next state to complete the transition
		nextState.Enter(ev)
	} else {
		// Diffs remain: go to SEND_DELTA
		nextState := &InitiatorSendDeltaState{fsm: s.fsm}
		s.fsm.initiatorState = nextState
		// Call Enter on the next state to complete the transition
		nextState.Enter(ev)
	}
}

func (s *InitiatorComputeMissingState) Transition(ev *IStateEvent) FSMInitiatorState {
	// This state immediately transitions on enter, so no external events are handled
	return s
}

func (s *InitiatorComputeMissingState) GetState() InitiatorState {
	return IComputeMissing
}

// InitiatorSendDeltaState sends delta operations to responder
type InitiatorSendDeltaState struct {
	fsm *FSM
}

func (s *InitiatorSendDeltaState) Enter(ev *IStateEvent) {
	// SendChunk(); ArmTimer()
	// Send chunk would be implemented here
	s.fsm.bus.Publish(&SendChunkEvent{
		ChunkData: []byte("delta_chunk"),
		Seq:       1,
		Last:      false,
	})
	// ArmTimer() would be implemented here
	s.fsm.bus.Publish(&ArmTimerEvent{
		TimerType: "delta_timeout",
		Duration:  3000, // 3 seconds
	})

	// On enter, immediately transition to WAIT_ACK
	nextState := &InitiatorWaitAckState{fsm: s.fsm}
	s.fsm.initiatorState = nextState
	// Call Enter on the next state to complete the transition
	nextState.Enter(ev)
}

func (s *InitiatorSendDeltaState) Transition(ev *IStateEvent) FSMInitiatorState {
	// This state immediately transitions on enter, so no external events are handled
	return s
}

func (s *InitiatorSendDeltaState) GetState() InitiatorState {
	return ISendDelta
}

// InitiatorWaitAckState waits for acknowledgment from responder
type InitiatorWaitAckState struct {
	fsm *FSM
}

func (s *InitiatorWaitAckState) Enter(ev *IStateEvent) {
	// Waiting for acknowledgment
}

func (s *InitiatorWaitAckState) Transition(ev *IStateEvent) FSMInitiatorState {
	switch ev.Type {
	case IAck:
		// DisarmTimer() action
		s.fsm.bus.Publish(&DisarmTimerEvent{TimerType: "delta_timeout"})

		// Check guard: moreChunks
		if ackEvent, ok := ev.Data.(*AckEvent); ok {
			if ackEvent.MoreChunks {
				// moreChunks: go back to SEND_DELTA
				next := &InitiatorSendDeltaState{fsm: s.fsm}
				next.Enter(ev)
				return next
			} else {
				// noMoreChunks: go to NO_MORE
				next := &InitiatorNoMoreState{fsm: s.fsm}
				next.Enter(ev)
				return next
			}
		}
		return s
	case ITimeout:
		// Timeout: retransmit and go back to SEND_DELTA
		// Retransmit() action
		s.fsm.bus.Publish(&RetransmitEvent{
			DataType: "delta_chunk",
			Data:     ev.Data,
		})
		next := &InitiatorSendDeltaState{fsm: s.fsm}
		next.Enter(ev)
		return next
	default:
		return s
	}
}

func (s *InitiatorWaitAckState) GetState() InitiatorState {
	return IWaitAck
}

// InitiatorNoMoreState handles the no-more-operations state
type InitiatorNoMoreState struct {
	fsm *FSM
}

func (s *InitiatorNoMoreState) Enter(ev *IStateEvent) {
	// No more operations to send

	// Check guard: diffsRemain
	if s.fsm.diffsRemain {
		// diffsRemain: start another mini-round, go to SEND_SUMMARY
		nextState := &InitiatorSendSummaryState{fsm: s.fsm}
		s.fsm.initiatorState = nextState
		// Call Enter on the next state to complete the transition
		nextState.Enter(ev)
	} else {
		// !diffsRemain: go to COMMIT_CLOSE
		nextState := &InitiatorCommitCloseState{fsm: s.fsm}
		s.fsm.initiatorState = nextState
		// Call Enter on the next state to complete the transition
		nextState.Enter(ev)
	}
}

func (s *InitiatorNoMoreState) Transition(ev *IStateEvent) FSMInitiatorState {
	// This state immediately transitions on enter, so no external events are handled
	return s
}

func (s *InitiatorNoMoreState) GetState() InitiatorState {
	return INoMore
}

// InitiatorCommitCloseState handles the commit and close state
type InitiatorCommitCloseState struct {
	fsm *FSM
}

func (s *InitiatorCommitCloseState) Enter(ev *IStateEvent) {
	// CloseSession()
	// Close session would be implemented here
	s.fsm.bus.Publish(&CloseSessionEvent{
		SessionID: "session_123",
	})
}

func (s *InitiatorCommitCloseState) Transition(ev *IStateEvent) FSMInitiatorState {
	switch ev.Type {
	case IEndsyncRx:
		// EndsyncRx: go back to IDLE
		next := &InitiatorIdleState{fsm: s.fsm}
		next.Enter(ev)
		return next
	default:
		return s
	}
}

func (s *InitiatorCommitCloseState) GetState() InitiatorState {
	return ICommitClose
}

// =============================================================================
// Responder State Implementations (following the transition table exactly)
// =============================================================================

// ResponderIdleState represents the idle state for the responder
type ResponderIdleState struct {
	fsm *FSM
}

func (s *ResponderIdleState) Enter(ev *RStateEvent) {
	// No action needed in idle state
}

func (s *ResponderIdleState) Transition(ev *RStateEvent) FSMResponderState {
	switch ev.Type {
	case RIncomingSession:
		next := &ResponderSessionAcceptState{fsm: s.fsm}
		next.Enter(ev)
		return next
	default:
		return s
	}
}

func (s *ResponderIdleState) GetState() ResponderState {
	return RIdle
}

// ResponderSessionAcceptState represents the session accept state for the responder
type ResponderSessionAcceptState struct {
	fsm *FSM
}

func (s *ResponderSessionAcceptState) Enter(ev *RStateEvent) {
	// Session accepted, waiting for parameters
}

func (s *ResponderSessionAcceptState) Transition(ev *RStateEvent) FSMResponderState {
	switch ev.Type {
	case RParamsOK:
		next := &ResponderRecvSummaryState{fsm: s.fsm}
		next.Enter(ev)
		return next
	default:
		return s
	}
}

func (s *ResponderSessionAcceptState) GetState() ResponderState {
	return RSessionAccept
}

// ResponderRecvSummaryState handles receiving summary from initiator
type ResponderRecvSummaryState struct {
	fsm *FSM
}

func (s *ResponderRecvSummaryState) Enter(ev *RStateEvent) {
	// Summary received, ready to perform diff descent
}

func (s *ResponderRecvSummaryState) Transition(ev *RStateEvent) FSMResponderState {
	switch ev.Type {
	case RSummaryReq:
		// ReplySummary() action would be implemented here
		s.fsm.bus.Publish(&ReplySummaryEvent{
			Summary: []byte("summary_response"),
			RootEq:  true, // This would be computed based on actual state
		})
		next := &ResponderDiffDescentState{fsm: s.fsm}
		next.Enter(ev)
		return next
	default:
		return s
	}
}

func (s *ResponderRecvSummaryState) GetState() ResponderState {
	return RRecvSummary
}

// ResponderDiffDescentState performs diff descent on the summary
type ResponderDiffDescentState struct {
	fsm *FSM
}

func (s *ResponderDiffDescentState) Enter(ev *RStateEvent) {
	// Perform diff descent to identify differences
}

func (s *ResponderDiffDescentState) Transition(ev *RStateEvent) FSMResponderState {
	switch ev.Type {
	case RChildrenReq:
		// ReplyChildren() action would be implemented here
		s.fsm.bus.Publish(&ReplyChildrenEvent{
			Children: []byte("children_data"),
		})
		next := &ResponderApplyProofsState{fsm: s.fsm}
		next.Enter(ev)
		return next
	case RLeavesReq:
		// ReplyLeaves() action would be implemented here
		s.fsm.bus.Publish(&ReplyLeavesEvent{
			Leaves: []byte("leaves_data"),
		})
		next := &ResponderApplyProofsState{fsm: s.fsm}
		next.Enter(ev)
		return next
	default:
		return s
	}
}

func (s *ResponderDiffDescentState) GetState() ResponderState {
	return RDiffDescent
}

// ResponderApplyProofsState applies proofs from initiator
type ResponderApplyProofsState struct {
	fsm *FSM
}

func (s *ResponderApplyProofsState) Enter(ev *RStateEvent) {
	// prepare per-leaf SV fence
	s.fsm.bus.Publish(&PrepareSVFenceEvent{
		Leaves: []byte("leaf_data"),
	})
	// On enter, immediately transition to RECV_DELTA
	nextState := &ResponderRecvDeltaState{fsm: s.fsm}
	s.fsm.responderState = nextState
	// Call Enter on the next state to complete the transition
	nextState.Enter(ev)
}

func (s *ResponderApplyProofsState) Transition(ev *RStateEvent) FSMResponderState {
	// This state immediately transitions on enter, so no external events are handled
	return s
}

func (s *ResponderApplyProofsState) GetState() ResponderState {
	return RApplyProofs
}

// ResponderRecvDeltaState receives delta operations from initiator
type ResponderRecvDeltaState struct {
	fsm *FSM
}

func (s *ResponderRecvDeltaState) Enter(ev *RStateEvent) {
	// Receive delta operations
}

func (s *ResponderRecvDeltaState) Transition(ev *RStateEvent) FSMResponderState {
	switch ev.Type {
	case RDeltaChunk:
		// StageChunk() action would be implemented here
		if deltaChunk, ok := ev.Data.(*DeltaChunkEvent); ok {
			s.fsm.bus.Publish(&StageChunkEvent{
				ChunkData: deltaChunk.Data,
				Seq:       deltaChunk.Seq,
				Last:      deltaChunk.Last,
			})
		}
		next := &ResponderApplyOpsState{fsm: s.fsm}
		next.Enter(ev)
		return next
	default:
		return s
	}
}

func (s *ResponderRecvDeltaState) GetState() ResponderState {
	return RRecvDelta
}

// ResponderApplyOpsState applies received operations
type ResponderApplyOpsState struct {
	fsm *FSM
}

func (s *ResponderApplyOpsState) Enter(ev *RStateEvent) {
	// Apply received operations
	// *(contiguous ready)* - this would be a guard condition
	// For now, we'll assume it's ready and transition to SEND_ACK
	// ApplyDeterministic(); SendAck(hseq) actions would be implemented here
	s.fsm.bus.Publish(&ApplyDeterministicEvent{
		Operations: []byte("operations_to_apply"),
	})

	s.fsm.bus.Publish(&SendAckEvent{
		HSeq: 1, // This would be computed based on actual state
	})

	nextState := &ResponderSendAckState{fsm: s.fsm}
	s.fsm.responderState = nextState
	// Call Enter on the next state to complete the transition
	nextState.Enter(ev)
}

func (s *ResponderApplyOpsState) Transition(ev *RStateEvent) FSMResponderState {
	// This state immediately transitions on enter, so no external events are handled
	return s
}

func (s *ResponderApplyOpsState) GetState() ResponderState {
	return RApplyOps
}

// ResponderSendAckState sends acknowledgment to initiator
type ResponderSendAckState struct {
	fsm *FSM
}

func (s *ResponderSendAckState) Enter(ev *RStateEvent) {
	// Send acknowledgment to initiator
	// On enter, immediately transition to CHECK_DIFFS
	nextState := &ResponderCheckDiffsState{fsm: s.fsm}
	s.fsm.responderState = nextState
	// Call Enter on the next state to complete the transition
	nextState.Enter(ev)
}

func (s *ResponderSendAckState) Transition(ev *RStateEvent) FSMResponderState {
	// This state immediately transitions on enter, so no external events are handled
	return s
}

func (s *ResponderSendAckState) GetState() ResponderState {
	return RSendAck
}

// ResponderCheckDiffsState checks for remaining differences
type ResponderCheckDiffsState struct {
	fsm *FSM
}

func (s *ResponderCheckDiffsState) Enter(ev *RStateEvent) {
	// Check if there are remaining differences
	// Check guard: diffsRemain
	if s.fsm.diffsRemain {
		// diffsRemain: go back to DIFF_DESCENT
		nextState := &ResponderDiffDescentState{fsm: s.fsm}
		s.fsm.responderState = nextState
		// Call Enter on the next state to complete the transition
		nextState.Enter(ev)
	} else {
		// !diffsRemain: go to COMMIT_CLOSE
		nextState := &ResponderCommitCloseState{fsm: s.fsm}
		s.fsm.responderState = nextState
		// Call Enter on the next state to complete the transition
		nextState.Enter(ev)
	}
}

func (s *ResponderCheckDiffsState) Transition(ev *RStateEvent) FSMResponderState {
	// This state immediately transitions on enter, so no external events are handled
	return s
}

func (s *ResponderCheckDiffsState) GetState() ResponderState {
	return RCheckDiffs
}

// ResponderCommitCloseState handles the commit and close state
type ResponderCommitCloseState struct {
	fsm *FSM
}

func (s *ResponderCommitCloseState) Enter(ev *RStateEvent) {
	// Commit changes and close session
}

func (s *ResponderCommitCloseState) Transition(ev *RStateEvent) FSMResponderState {
	switch ev.Type {
	case REndsyncTx:
		// EndsyncTx: go back to IDLE
		// cleanup action
		s.fsm.bus.Publish(&EndsyncTxEvent{
			SessionID: "session_123",
		})
		next := &ResponderIdleState{fsm: s.fsm}
		next.Enter(ev)
		return next
	default:
		return s
	}
}

func (s *ResponderCommitCloseState) GetState() ResponderState {
	return RCommitClose
}
