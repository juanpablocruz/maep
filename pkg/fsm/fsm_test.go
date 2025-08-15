package fsm

import (
	"sync"
	"testing"
	"time"

	"github.com/juanpablocruz/maep/pkg/eventbus"
)

// Test_FSM_INIT_01_InitialState validates the FSM starts in the correct initial states
func Test_FSM_INIT_01_InitialState(t *testing.T) {
	// ID: FSM-INIT-01
	// Target: FSM (State Machine)
	// Setup: Create a new FSM instance
	// Stimulus: Check initial states
	// Checks: Both initiator and responder should start in idle state

	bus := eventbus.NewEventBus()
	fsm := NewFSM(bus)

	// Verify initial states
	if fsm.GetInitiatorState() != IIdle {
		t.Errorf("Expected initiator to start in IIdle, got %v", fsm.GetInitiatorState())
	}

	if fsm.GetResponderState() != RIdle {
		t.Errorf("Expected responder to start in RIdle, got %v", fsm.GetResponderState())
	}
}

// Test_FSM_TRANS_01_InitiatorTransitions validates initiator state transitions according to the transition table
func Test_FSM_TRANS_01_InitiatorTransitions(t *testing.T) {
	// ID: FSM-TRANS-01
	// Target: FSM (State Machine)
	// Setup: Create FSM and track state changes
	// Stimulus: Send events to trigger state transitions following the transition table
	// Checks: States transition correctly according to the specification

	bus := eventbus.NewEventBus()
	fsm := NewFSM(bus)

	// Test IDLE → SESSION_OPEN (StartSession)
	fsm.HandleInitiatorEvent(&IStateEvent{Type: IStartSession})
	if fsm.GetInitiatorState() != ISessionOpen {
		t.Errorf("Expected ISessionOpen, got %v", fsm.GetInitiatorState())
	}

	// Test SESSION_OPEN → SEND_SUMMARY (ConnOK)
	fsm.HandleInitiatorEvent(&IStateEvent{Type: IConnOK})
	// SEND_SUMMARY immediately transitions to WAIT_PROOFS on enter
	if fsm.GetInitiatorState() != IWaitProofs {
		t.Errorf("Expected IWaitProofs, got %v", fsm.GetInitiatorState())
	}

	// Test WAIT_PROOFS → NO_MORE (SummaryResp with rootEq=true)
	fsm.HandleInitiatorEvent(&IStateEvent{
		Type: ISummaryResp,
		Data: &SummaryResponseEvent{RootEq: true},
	})
	// NO_MORE immediately transitions based on diffsRemain guard
	// Since diffsRemain is false by default, it should go to COMMIT_CLOSE
	if fsm.GetInitiatorState() != ICommitClose {
		t.Errorf("Expected ICommitClose, got %v", fsm.GetInitiatorState())
	}

	// Test COMMIT_CLOSE → IDLE (EndsyncRx)
	fsm.HandleInitiatorEvent(&IStateEvent{Type: IEndsyncRx})
	if fsm.GetInitiatorState() != IIdle {
		t.Errorf("Expected IIdle, got %v", fsm.GetInitiatorState())
	}
}

// Test_FSM_TRANS_02_InitiatorAlternativePath tests the alternative path with diffs
func Test_FSM_TRANS_02_InitiatorAlternativePath(t *testing.T) {
	// ID: FSM-TRANS-02
	// Target: FSM (State Machine)
	// Setup: Create FSM and follow the path with differences
	// Stimulus: Send events to trigger the alternative path
	// Checks: States transition correctly for the path with differences

	bus := eventbus.NewEventBus()
	fsm := NewFSM(bus)

	// Start session
	fsm.HandleInitiatorEvent(&IStateEvent{Type: IStartSession})
	fsm.HandleInitiatorEvent(&IStateEvent{Type: IConnOK})

	// Test WAIT_PROOFS → COMPUTE_MISSING (SummaryResp with rootEq=false)
	fsm.HandleInitiatorEvent(&IStateEvent{
		Type: ISummaryResp,
		Data: &SummaryResponseEvent{RootEq: false},
	})
	// COMPUTE_MISSING immediately transitions based on diffsRemain guard
	// Since diffsRemain is true (set in Enter), it should go to SEND_DELTA
	// SEND_DELTA immediately transitions to WAIT_ACK on enter
	if fsm.GetInitiatorState() != IWaitAck {
		t.Errorf("Expected IWaitAck, got %v", fsm.GetInitiatorState())
	}

	// Test WAIT_ACK → SEND_DELTA (Ack with moreChunks=true)
	fsm.HandleInitiatorEvent(&IStateEvent{
		Type: IAck,
		Data: &AckEvent{MoreChunks: true},
	})
	// SEND_DELTA immediately transitions to WAIT_ACK on enter
	if fsm.GetInitiatorState() != IWaitAck {
		t.Errorf("Expected IWaitAck, got %v", fsm.GetInitiatorState())
	}

	// Test WAIT_ACK → NO_MORE (Ack with moreChunks=false)
	fsm.HandleInitiatorEvent(&IStateEvent{
		Type: IAck,
		Data: &AckEvent{MoreChunks: false},
	})
	// NO_MORE immediately transitions based on diffsRemain guard
	// Since diffsRemain is true, it should go to SEND_SUMMARY
	// SEND_SUMMARY immediately transitions to WAIT_PROOFS on enter
	if fsm.GetInitiatorState() != IWaitProofs {
		t.Errorf("Expected IWaitProofs, got %v", fsm.GetInitiatorState())
	}
}

// Test_FSM_TRANS_03_ResponderTransitions validates responder state transitions
func Test_FSM_TRANS_03_ResponderTransitions(t *testing.T) {
	// ID: FSM-TRANS-03
	// Target: FSM (State Machine)
	// Setup: Create FSM and track state changes
	// Stimulus: Send events to trigger state transitions
	// Checks: States transition correctly and stay in current state for invalid events

	bus := eventbus.NewEventBus()
	fsm := NewFSM(bus)

	// Test IDLE → SESSION_ACCEPT (IncomingSession)
	fsm.HandleResponderEvent(&RStateEvent{Type: RIncomingSession})
	if fsm.GetResponderState() != RSessionAccept {
		t.Errorf("Expected RSessionAccept, got %v", fsm.GetResponderState())
	}

	// Test SESSION_ACCEPT → RECV_SUMMARY (ParamsOK)
	fsm.HandleResponderEvent(&RStateEvent{Type: RParamsOK})
	if fsm.GetResponderState() != RRecvSummary {
		t.Errorf("Expected RRecvSummary, got %v", fsm.GetResponderState())
	}

	// Test RECV_SUMMARY → DIFF_DESCENT (SummaryReq)
	fsm.HandleResponderEvent(&RStateEvent{Type: RSummaryReq})
	if fsm.GetResponderState() != RDiffDescent {
		t.Errorf("Expected RDiffDescent, got %v", fsm.GetResponderState())
	}

	// Test DIFF_DESCENT → APPLY_PROOFS (ChildrenReq)
	fsm.HandleResponderEvent(&RStateEvent{Type: RChildrenReq})
	// APPLY_PROOFS immediately transitions to RECV_DELTA on enter
	if fsm.GetResponderState() != RRecvDelta {
		t.Errorf("Expected RRecvDelta, got %v", fsm.GetResponderState())
	}

	// Test RECV_DELTA → APPLY_OPS (DeltaChunk)
	// Set diffsRemain to true so the chain goes to DIFF_DESCENT
	fsm.SetDiffsRemain(true)
	fsm.HandleResponderEvent(&RStateEvent{
		Type: RDeltaChunk,
		Data: &DeltaChunkEvent{Seq: 1, Last: false},
	})
	// APPLY_OPS immediately transitions to SEND_ACK on enter, then to CHECK_DIFFS
	// Since diffsRemain is true, it should go to DIFF_DESCENT
	if fsm.GetResponderState() != RDiffDescent {
		t.Errorf("Expected RDiffDescent, got %v", fsm.GetResponderState())
	}

	// Test the complete flow to COMMIT_CLOSE
	// First, go through APPLY_PROOFS → RECV_DELTA → APPLY_OPS → SEND_ACK → CHECK_DIFFS
	fsm.HandleResponderEvent(&RStateEvent{Type: RChildrenReq})
	fsm.HandleResponderEvent(&RStateEvent{
		Type: RDeltaChunk,
		Data: &DeltaChunkEvent{Seq: 2, Last: false},
	})
	// Now we should be in CHECK_DIFFS with diffsRemain = true, so it should go to DIFF_DESCENT
	if fsm.GetResponderState() != RDiffDescent {
		t.Errorf("Expected RDiffDescent, got %v", fsm.GetResponderState())
	}

	// Now set diffsRemain = false and go through the flow again to reach COMMIT_CLOSE
	fsm.SetDiffsRemain(false)
	fsm.HandleResponderEvent(&RStateEvent{Type: RChildrenReq})
	fsm.HandleResponderEvent(&RStateEvent{
		Type: RDeltaChunk,
		Data: &DeltaChunkEvent{Seq: 3, Last: false},
	})
	// Now we should be in COMMIT_CLOSE
	if fsm.GetResponderState() != RCommitClose {
		t.Errorf("Expected RCommitClose, got %v", fsm.GetResponderState())
	}

	// Test COMMIT_CLOSE → IDLE (EndsyncTx)
	fsm.HandleResponderEvent(&RStateEvent{Type: REndsyncTx})
	if fsm.GetResponderState() != RIdle {
		t.Errorf("Expected RIdle, got %v", fsm.GetResponderState())
	}
}

// Test_FSM_INV_01_StateInvariants validates that states remain stable for invalid events
func Test_FSM_INV_01_StateInvariants(t *testing.T) {
	// ID: FSM-INV-01
	// Target: FSM (State Machine)
	// Setup: Create FSM in specific states
	// Stimulus: Send invalid events
	// Checks: States remain unchanged for invalid transitions

	bus := eventbus.NewEventBus()
	fsm := NewFSM(bus)

	// Test that invalid events don't change state
	initialInitiatorState := fsm.GetInitiatorState()
	initialResponderState := fsm.GetResponderState()

	// Send invalid event
	fsm.HandleInitiatorEvent(&IStateEvent{Type: IAck}) // Invalid from idle
	if fsm.GetInitiatorState() != initialInitiatorState {
		t.Errorf("State should remain unchanged for invalid event, got %v", fsm.GetInitiatorState())
	}

	fsm.HandleResponderEvent(&RStateEvent{Type: RDeltaChunk}) // Invalid from idle
	if fsm.GetResponderState() != initialResponderState {
		t.Errorf("State should remain unchanged for invalid event, got %v", fsm.GetResponderState())
	}
}

// Test_FSM_EVENT_01_EventBusIntegration validates event bus integration
func Test_FSM_EVENT_01_EventBusIntegration(t *testing.T) {
	// ID: FSM-EVENT-01
	// Target: FSM (State Machine)
	// Setup: Create FSM with event bus and subscribe to events
	// Stimulus: Trigger state that publishes events
	// Checks: Events are published correctly

	bus := eventbus.NewEventBus()
	fsm := NewFSM(bus)

	// Create a test subscriber
	eventReceived := false
	subscriber := &testSubscriber{
		eventChan: make(chan eventbus.Event, 1),
		waitGroup: &sync.WaitGroup{},
		onEvent: func(event eventbus.Event) {
			if summaryEvent, ok := event.(*SendSummaryEvent); ok {
				if string(summaryEvent.Summary) == "test summary" {
					eventReceived = true
				}
			}
		},
	}

	// Subscribe and start the event bus
	bus.Subscribe(subscriber)
	bus.Start()

	// Trigger state that sends summary
	fsm.HandleInitiatorEvent(&IStateEvent{Type: IStartSession})
	fsm.HandleInitiatorEvent(&IStateEvent{Type: IConnOK, Data: []byte("test summary")})

	// Wait for event processing
	subscriber.waitGroup.Wait()

	// Verify event was published
	if !eventReceived {
		t.Error("Expected SendSummaryEvent to be published")
	}

	// Clean up
	bus.Stop()
}

// Test_FSM_TIMEOUT_01_TimeoutHandling validates timeout handling
func Test_FSM_TIMEOUT_01_TimeoutHandling(t *testing.T) {
	// ID: FSM-TIMEOUT-01
	// Target: FSM (State Machine)
	// Setup: Create FSM and put it in WAIT_PROOFS state
	// Stimulus: Send timeout event
	// Checks: State transitions back to SEND_SUMMARY for retransmission

	bus := eventbus.NewEventBus()
	fsm := NewFSM(bus)

	// Get to WAIT_PROOFS state
	fsm.HandleInitiatorEvent(&IStateEvent{Type: IStartSession})
	fsm.HandleInitiatorEvent(&IStateEvent{Type: IConnOK})

	// Verify we're in WAIT_PROOFS
	if fsm.GetInitiatorState() != IWaitProofs {
		t.Errorf("Expected IWaitProofs, got %v", fsm.GetInitiatorState())
	}

	// Send timeout event
	fsm.HandleInitiatorEvent(&IStateEvent{Type: ITimeout})

	// Should transition back to SEND_SUMMARY for retransmission
	// SEND_SUMMARY immediately transitions to WAIT_PROOFS on enter
	if fsm.GetInitiatorState() != IWaitProofs {
		t.Errorf("Expected IWaitProofs after timeout, got %v", fsm.GetInitiatorState())
	}
}

// Test_FSM_ACTIONS_01_ActionEventEmission validates that all action events are emitted correctly
func Test_FSM_ACTIONS_01_ActionEventEmission(t *testing.T) {
	// ID: FSM-ACTIONS-01
	// Target: FSM (State Machine)
	// Setup: Create FSM with event bus and subscribe to action events
	// Stimulus: Trigger state transitions that should emit action events
	// Checks: All expected action events are published through the event bus

	bus := eventbus.NewEventBus()
	fsm := NewFSM(bus)

	// Track emitted events
	emittedEvents := make(map[string]bool)
	emittedEventsMutex := &sync.Mutex{}

	// Create subscribers for all action events
	actionEvents := []string{
		"SendSummaryEvent", "ArmTimerEvent", "DisarmTimerEvent", "RetransmitEvent",
		"SendChunkEvent", "CloseSessionEvent", "DescendDiffEvent", "PlanDeltasEvent",
		"ReplySummaryEvent", "ReplyChildrenEvent", "ReplyLeavesEvent", "PrepareSVFenceEvent",
		"StageChunkEvent", "ApplyDeterministicEvent", "SendAckEvent", "EndsyncTxEvent",
	}

	// Subscribe to all action events
	for _, eventType := range actionEvents {
		eventType := eventType // capture for closure
		subscriber := &testSubscriber{
			eventChan: make(chan eventbus.Event, 10),
			waitGroup: &sync.WaitGroup{},
			onEvent: func(event eventbus.Event) {
				if event.GetType() == eventType {
					emittedEventsMutex.Lock()
					emittedEvents[eventType] = true
					emittedEventsMutex.Unlock()
				}
			},
		}
		bus.Subscribe(subscriber)
	}

	// Start the event bus
	bus.Start()

	// Test Initiator actions - trigger a complete flow
	fsm.HandleInitiatorEvent(&IStateEvent{Type: IStartSession})
	fsm.HandleInitiatorEvent(&IStateEvent{Type: IConnOK, Data: []byte("test summary")})

	// Wait for event processing
	time.Sleep(100 * time.Millisecond)

	// Verify SendSummary and ArmTimer events were emitted
	emittedEventsMutex.Lock()
	sendSummaryEmitted := emittedEvents["SendSummaryEvent"]
	armTimerEmitted := emittedEvents["ArmTimerEvent"]
	emittedEventsMutex.Unlock()

	if !sendSummaryEmitted {
		t.Error("Expected SendSummaryEvent to be emitted")
	}
	if !armTimerEmitted {
		t.Error("Expected ArmTimerEvent to be emitted")
	}

	// Test timeout handling
	fsm.HandleInitiatorEvent(&IStateEvent{Type: ITimeout})
	time.Sleep(100 * time.Millisecond)

	// Verify Retransmit event was emitted
	emittedEventsMutex.Lock()
	retransmitEmitted := emittedEvents["RetransmitEvent"]
	emittedEventsMutex.Unlock()

	if !retransmitEmitted {
		t.Error("Expected RetransmitEvent to be emitted")
	}

	// Test summary response handling
	fsm.HandleInitiatorEvent(&IStateEvent{
		Type: ISummaryResp,
		Data: &SummaryResponseEvent{RootEq: false},
	})
	time.Sleep(100 * time.Millisecond)

	// Verify DisarmTimer, DescendDiff, and PlanDeltas events were emitted
	emittedEventsMutex.Lock()
	disarmTimerEmitted := emittedEvents["DisarmTimerEvent"]
	descendDiffEmitted := emittedEvents["DescendDiffEvent"]
	planDeltasEmitted := emittedEvents["PlanDeltasEvent"]
	emittedEventsMutex.Unlock()

	if !disarmTimerEmitted {
		t.Error("Expected DisarmTimerEvent to be emitted")
	}
	if !descendDiffEmitted {
		t.Error("Expected DescendDiffEvent to be emitted")
	}
	if !planDeltasEmitted {
		t.Error("Expected PlanDeltasEvent to be emitted")
	}

	// Test Responder actions - trigger a complete flow
	fsm.HandleResponderEvent(&RStateEvent{Type: RIncomingSession})
	fsm.HandleResponderEvent(&RStateEvent{Type: RParamsOK})
	fsm.HandleResponderEvent(&RStateEvent{Type: RSummaryReq})
	time.Sleep(100 * time.Millisecond)

	// Verify ReplySummary event was emitted
	emittedEventsMutex.Lock()
	replySummaryEmitted := emittedEvents["ReplySummaryEvent"]
	emittedEventsMutex.Unlock()

	if !replySummaryEmitted {
		t.Error("Expected ReplySummaryEvent to be emitted")
	}

	// Test children request
	fsm.HandleResponderEvent(&RStateEvent{Type: RChildrenReq})
	time.Sleep(100 * time.Millisecond)

	// Verify ReplyChildren and PrepareSVFence events were emitted
	emittedEventsMutex.Lock()
	replyChildrenEmitted := emittedEvents["ReplyChildrenEvent"]
	prepareSVFenceEmitted := emittedEvents["PrepareSVFenceEvent"]
	emittedEventsMutex.Unlock()

	if !replyChildrenEmitted {
		t.Error("Expected ReplyChildrenEvent to be emitted")
	}
	if !prepareSVFenceEmitted {
		t.Error("Expected PrepareSVFenceEvent to be emitted")
	}

	// Test delta chunk handling
	fsm.HandleResponderEvent(&RStateEvent{
		Type: RDeltaChunk,
		Data: &DeltaChunkEvent{Seq: 1, Last: false, Data: []byte("chunk data")},
	})
	time.Sleep(100 * time.Millisecond)

	// Verify StageChunk, ApplyDeterministic, and SendAck events were emitted
	emittedEventsMutex.Lock()
	stageChunkEmitted := emittedEvents["StageChunkEvent"]
	applyDeterministicEmitted := emittedEvents["ApplyDeterministicEvent"]
	sendAckEmitted := emittedEvents["SendAckEvent"]
	emittedEventsMutex.Unlock()

	if !stageChunkEmitted {
		t.Error("Expected StageChunkEvent to be emitted")
	}
	if !applyDeterministicEmitted {
		t.Error("Expected ApplyDeterministicEvent to be emitted")
	}
	if !sendAckEmitted {
		t.Error("Expected SendAckEvent to be emitted")
	}

	// Test endsync - first get to COMMIT_CLOSE state
	fsm.SetDiffsRemain(false)
	fsm.HandleResponderEvent(&RStateEvent{Type: RChildrenReq})
	fsm.HandleResponderEvent(&RStateEvent{
		Type: RDeltaChunk,
		Data: &DeltaChunkEvent{Seq: 2, Last: false, Data: []byte("final chunk")},
	})
	time.Sleep(100 * time.Millisecond)

	// Now send the endsync event
	fsm.HandleResponderEvent(&RStateEvent{Type: REndsyncTx})
	time.Sleep(100 * time.Millisecond)

	// Verify EndsyncTx event was emitted
	emittedEventsMutex.Lock()
	endsyncTxEmitted := emittedEvents["EndsyncTxEvent"]
	emittedEventsMutex.Unlock()

	if !endsyncTxEmitted {
		t.Error("Expected EndsyncTxEvent to be emitted")
	}

	// Clean up
	bus.Stop()
}

// testSubscriber implements the eventbus.Subscriber interface for testing
type testSubscriber struct {
	eventChan chan eventbus.Event
	waitGroup *sync.WaitGroup
	onEvent   func(eventbus.Event)
}

func (t *testSubscriber) OnEvent(event eventbus.Event) {
	if t.onEvent != nil {
		t.onEvent(event)
	}
}

func (t *testSubscriber) GetChannel() chan eventbus.Event {
	return t.eventChan
}

func (t *testSubscriber) GetWaitGroup() *sync.WaitGroup {
	return t.waitGroup
}
