// Package fsm implements a finite state machine in charge of managing the node's state machine.
package fsm

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
	InitiatorStateIdle InitiatorState = iota
	InitiatorStateSessionOpen
	InitiatorStateSendSummary
	InitiatorStateWaitProofs
	InitiatorStateComputeMissing
	InitiatorStateSendDelta
	InitiatorStateWaitAck
	InitiatorStateNoMore
	InitiatorStateCommitClose
)

type ResponderState int

const (
	ResponderStateIdle ResponderState = iota
	ResponderStateSessionAccept
	ResponderStateRecvSummary
	ResponderStateDiffDescent
	ResponderStateApplyProofs
	ResponderStateRecvDelta
	ResponderStateApplyOps
	ResponderStateSendAck
	ResponderStateCheckDiffs
	ResponderStateCommitClose
)

type FSM struct {
	initiatorState InitiatorState
	responderState ResponderState
}

func (f *FSM) HandleInitiatorEvent(ev *InitiatorStateEvent) {
}

func (f *FSM) HandleResponderEvent(ev *ResponderStateEvent) {

}

func NewFSM() *FSM {
	return &FSM{
		initiatorState: InitiatorStateIdle,
		responderState: ResponderStateIdle,
	}
}
