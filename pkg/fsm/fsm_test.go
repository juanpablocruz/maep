package fsm

import (
	"sync"
	"testing"
	"time"

	"github.com/juanpablocruz/maep/pkg/eventbus"
	"github.com/juanpablocruz/maep/pkg/timer"
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

// Test_FSM_TIMER_01_TimerIntegration validates FSM integration with timer subscriber
func Test_FSM_TIMER_01_TimerIntegration(t *testing.T) {
	// ID: FSM-TIMER-01
	// Target: FSM (State Machine) + Timer Subscriber
	// Setup: Create FSM with timer subscriber and event bus
	// Stimulus: Trigger states that arm timers and handle timeouts
	// Checks: Timers are armed/disarmed correctly and timeout events are handled

	bus := eventbus.NewEventBus()
	fsm := NewFSM(bus)
	timerSub := timer.NewTimerSubscriber(bus)

	// Track timer events
	var timerEventsMutex sync.Mutex
	timerEvents := make(map[string]bool)
	timeoutReceived := false

	// Create subscriber for timer events
	timerSubscriber := &testSubscriber{
		eventChan: make(chan eventbus.Event, 10),
		waitGroup: &sync.WaitGroup{},
		onEvent: func(event eventbus.Event) {
			if timerEvent, ok := event.(*TimerEvent); ok {
				timerEventsMutex.Lock()
				timerEvents[timerEvent.TimerType] = true
				timeoutReceived = true
				timerEventsMutex.Unlock()
			}
		},
	}

	// Subscribe and start
	bus.Subscribe(timerSubscriber)
	bus.Start()
	timerSub.Start()

	// Test 1: Initiator arms summary timer
	fsm.HandleInitiatorEvent(&IStateEvent{Type: IStartSession})
	fsm.HandleInitiatorEvent(&IStateEvent{Type: IConnOK, Data: []byte("test summary")})

	// Wait for timer to be armed
	time.Sleep(100 * time.Millisecond)

	// Check that summary timer is active
	activeTimers := timerSub.GetActiveTimers()
	found := false
	for _, timerType := range activeTimers {
		if timerType == "summary_timeout" {
			found = true
			break
		}
	}
	if !found {
		t.Error("Expected summary_timeout timer to be active")
	}

	// Test 2: Disarm timer via summary response
	fsm.HandleInitiatorEvent(&IStateEvent{
		Type: ISummaryResp,
		Data: &SummaryResponseEvent{RootEq: true},
	})

	// Wait for timer to be disarmed
	time.Sleep(100 * time.Millisecond)

	// Check that timer is no longer active
	activeTimers = timerSub.GetActiveTimers()
	found = false
	for _, timerType := range activeTimers {
		if timerType == "summary_timeout" {
			found = true
			break
		}
	}
	if found {
		t.Error("Expected summary_timeout timer to be disarmed")
	}

	// Test 3: Test timeout scenario
	// Start a new session and arm timer
	fsm.HandleInitiatorEvent(&IStateEvent{Type: IStartSession})
	fsm.HandleInitiatorEvent(&IStateEvent{Type: IConnOK, Data: []byte("test summary 2")})

	// Wait for timer to be armed
	time.Sleep(100 * time.Millisecond)

	// Manually trigger timeout by publishing TimerEvent
	bus.Publish(&TimerEvent{TimerType: "summary_timeout"})

	// Wait for timeout processing
	time.Sleep(100 * time.Millisecond)

	// Verify timeout was received
	timerEventsMutex.Lock()
	received := timeoutReceived
	timerEventsMutex.Unlock()

	if !received {
		t.Error("Expected timeout event to be received")
	}

	// Clean up
	timerSub.Stop()
	bus.Stop()
}

// Test_FSM_TIMER_02_TimeoutRetransmission validates timeout retransmission behavior
func Test_FSM_TIMER_02_TimeoutRetransmission(t *testing.T) {
	// ID: FSM-TIMER-02
	// Target: FSM (State Machine) + Timer Subscriber
	// Setup: Create FSM with timer subscriber
	// Stimulus: Trigger timeout and verify retransmission
	// Checks: FSM properly handles timeout and retransmits

	bus := eventbus.NewEventBus()
	fsm := NewFSM(bus)
	timerSub := timer.NewTimerSubscriber(bus)

	// Track retransmit events
	var retransmitCount int
	var retransmitMutex sync.Mutex

	retransmitSubscriber := &testSubscriber{
		eventChan: make(chan eventbus.Event, 10),
		waitGroup: &sync.WaitGroup{},
		onEvent: func(event eventbus.Event) {
			if _, ok := event.(*RetransmitEvent); ok {
				retransmitMutex.Lock()
				retransmitCount++
				retransmitMutex.Unlock()
			}
		},
	}

	// Subscribe and start
	bus.Subscribe(retransmitSubscriber)
	bus.Start()
	timerSub.Start()

	// Start session and arm timer
	fsm.HandleInitiatorEvent(&IStateEvent{Type: IStartSession})
	fsm.HandleInitiatorEvent(&IStateEvent{Type: IConnOK, Data: []byte("test summary")})

	// Verify we're in WAIT_PROOFS state
	if fsm.GetInitiatorState() != IWaitProofs {
		t.Errorf("Expected IWaitProofs, got %v", fsm.GetInitiatorState())
	}

	// Trigger timeout
	fsm.HandleInitiatorEvent(&IStateEvent{Type: ITimeout})

	// Wait for retransmission processing
	time.Sleep(100 * time.Millisecond)

	// Verify retransmit event was emitted
	retransmitMutex.Lock()
	count := retransmitCount
	retransmitMutex.Unlock()

	if count == 0 {
		t.Error("Expected RetransmitEvent to be emitted on timeout")
	}

	// Verify we're still in WAIT_PROOFS (ready for next response)
	if fsm.GetInitiatorState() != IWaitProofs {
		t.Errorf("Expected IWaitProofs after timeout, got %v", fsm.GetInitiatorState())
	}

	// Clean up
	timerSub.Stop()
	bus.Stop()
}

// Test_FSM_TIMER_03_DeltaChunkTimeout validates delta chunk timeout handling
func Test_FSM_TIMER_03_DeltaChunkTimeout(t *testing.T) {
	// ID: FSM-TIMER-03
	// Target: FSM (State Machine) + Timer Subscriber
	// Setup: Create FSM and get to WAIT_ACK state
	// Stimulus: Trigger timeout in WAIT_ACK state
	// Checks: Delta chunk timer is armed/disarmed correctly

	bus := eventbus.NewEventBus()
	fsm := NewFSM(bus)
	timerSub := timer.NewTimerSubscriber(bus)

	// Subscribe and start
	bus.Start()
	timerSub.Start()

	// Get to WAIT_ACK state through the initiator path
	fsm.HandleInitiatorEvent(&IStateEvent{Type: IStartSession})
	fsm.HandleInitiatorEvent(&IStateEvent{Type: IConnOK})
	fsm.HandleInitiatorEvent(&IStateEvent{
		Type: ISummaryResp,
		Data: &SummaryResponseEvent{RootEq: false},
	})

	// Verify we're in WAIT_ACK state
	if fsm.GetInitiatorState() != IWaitAck {
		t.Errorf("Expected IWaitAck, got %v", fsm.GetInitiatorState())
	}

	// Wait for delta chunk timer to be armed
	time.Sleep(100 * time.Millisecond)

	// Check that delta chunk timer is active
	activeTimers := timerSub.GetActiveTimers()
	found := false
	for _, timerType := range activeTimers {
		if timerType == "delta_timeout" {
			found = true
			break
		}
	}
	if !found {
		t.Error("Expected delta_timeout timer to be active")
	}

	// Test timeout handling
	fsm.HandleInitiatorEvent(&IStateEvent{Type: ITimeout})

	// Wait for retransmission processing
	time.Sleep(100 * time.Millisecond)

	// Verify we're still in WAIT_ACK (ready for next ack)
	if fsm.GetInitiatorState() != IWaitAck {
		t.Errorf("Expected IWaitAck after timeout, got %v", fsm.GetInitiatorState())
	}

	// Test ack handling (disarms timer)
	fsm.HandleInitiatorEvent(&IStateEvent{
		Type: IAck,
		Data: &AckEvent{MoreChunks: false},
	})

	// Wait for timer to be disarmed
	time.Sleep(100 * time.Millisecond)

	// Check that timer is no longer active
	activeTimers = timerSub.GetActiveTimers()
	found = false
	for _, timerType := range activeTimers {
		if timerType == "delta_timeout" {
			found = true
			break
		}
	}
	if found {
		t.Error("Expected delta_timeout timer to be disarmed")
	}

	// Clean up
	timerSub.Stop()
	bus.Stop()
}

// Test_FSM_TIMER_04_MultipleTimerScenarios validates multiple timer scenarios
func Test_FSM_TIMER_04_MultipleTimerScenarios(t *testing.T) {
	// ID: FSM-TIMER-04
	// Target: FSM (State Machine) + Timer Subscriber
	// Setup: Create FSM with timer subscriber
	// Stimulus: Test various timer scenarios including multiple timers
	// Checks: Multiple timers work correctly and don't interfere

	bus := eventbus.NewEventBus()
	fsm := NewFSM(bus)
	timerSub := timer.NewTimerSubscriber(bus)

	// Track timer events
	var timerEventsMutex sync.Mutex
	timerEvents := make(map[string]int)

	timerSubscriber := &testSubscriber{
		eventChan: make(chan eventbus.Event, 10),
		waitGroup: &sync.WaitGroup{},
		onEvent: func(event eventbus.Event) {
			if armEvent, ok := event.(*ArmTimerEvent); ok {
				timerEventsMutex.Lock()
				timerEvents[armEvent.TimerType]++
				timerEventsMutex.Unlock()
			}
		},
	}

	// Subscribe and start
	bus.Subscribe(timerSubscriber)
	bus.Start()
	timerSub.Start()

	// Test 1: Multiple summary timeouts (retransmissions)
	fsm.HandleInitiatorEvent(&IStateEvent{Type: IStartSession})
	fsm.HandleInitiatorEvent(&IStateEvent{Type: IConnOK, Data: []byte("test summary 1")})

	// Trigger timeout and retransmit
	fsm.HandleInitiatorEvent(&IStateEvent{Type: ITimeout})
	time.Sleep(100 * time.Millisecond)

	// Trigger another timeout
	fsm.HandleInitiatorEvent(&IStateEvent{Type: ITimeout})
	time.Sleep(100 * time.Millisecond)

	// Verify multiple summary timers were armed
	timerEventsMutex.Lock()
	summaryCount := timerEvents["summary_timeout"]
	timerEventsMutex.Unlock()

	if summaryCount < 2 {
		t.Errorf("Expected at least 2 summary timers to be armed, got %d", summaryCount)
	}

	// Test 2: Delta chunk timeouts
	fsm.HandleInitiatorEvent(&IStateEvent{
		Type: ISummaryResp,
		Data: &SummaryResponseEvent{RootEq: false},
	})

	// Trigger timeout in WAIT_ACK
	fsm.HandleInitiatorEvent(&IStateEvent{Type: ITimeout})
	time.Sleep(100 * time.Millisecond)

	// Trigger another timeout
	fsm.HandleInitiatorEvent(&IStateEvent{Type: ITimeout})
	time.Sleep(100 * time.Millisecond)

	// Verify delta chunk timers were armed
	timerEventsMutex.Lock()
	deltaCount := timerEvents["delta_timeout"]
	timerEventsMutex.Unlock()

	if deltaCount < 2 {
		t.Errorf("Expected at least 2 delta chunk timers to be armed, got %d", deltaCount)
	}

	// Test 3: Verify only one timer is active at a time
	activeTimers := timerSub.GetActiveTimers()
	if len(activeTimers) > 1 {
		t.Errorf("Expected at most 1 active timer, got %d: %v", len(activeTimers), activeTimers)
	}

	// Clean up
	timerSub.Stop()
	bus.Stop()
}

// Test_FSM_TIMER_05_TimerCleanup validates proper timer cleanup
func Test_FSM_TIMER_05_TimerCleanup(t *testing.T) {
	// ID: FSM-TIMER-05
	// Target: FSM (State Machine) + Timer Subscriber
	// Setup: Create FSM with timer subscriber and arm timers
	// Stimulus: Test various cleanup scenarios
	// Checks: Timers are properly cleaned up in all scenarios

	bus := eventbus.NewEventBus()
	fsm := NewFSM(bus)
	timerSub := timer.NewTimerSubscriber(bus)

	// Subscribe and start
	bus.Start()
	timerSub.Start()

	// Test 1: Arm timer and then disarm via normal flow
	fsm.HandleInitiatorEvent(&IStateEvent{Type: IStartSession})
	fsm.HandleInitiatorEvent(&IStateEvent{Type: IConnOK, Data: []byte("test summary")})

	// Wait for timer to be armed
	time.Sleep(100 * time.Millisecond)

	// Verify timer is active
	activeTimers := timerSub.GetActiveTimers()
	if len(activeTimers) == 0 {
		t.Error("Expected timer to be active")
	}

	// Disarm via summary response
	fsm.HandleInitiatorEvent(&IStateEvent{
		Type: ISummaryResp,
		Data: &SummaryResponseEvent{RootEq: true},
	})

	// Wait for disarm
	time.Sleep(100 * time.Millisecond)

	// Verify timer is disarmed
	activeTimers = timerSub.GetActiveTimers()
	if len(activeTimers) > 0 {
		t.Errorf("Expected no active timers, got %v", activeTimers)
	}

	// Test 2: Arm timer and stop timer subscriber
	fsm.HandleInitiatorEvent(&IStateEvent{Type: IStartSession})
	fsm.HandleInitiatorEvent(&IStateEvent{Type: IConnOK, Data: []byte("test summary 2")})

	// Wait for timer to be armed
	time.Sleep(100 * time.Millisecond)

	// Stop timer subscriber
	timerSub.Stop()

	// Verify all timers are cleaned up
	activeTimers = timerSub.GetActiveTimers()
	if len(activeTimers) > 0 {
		t.Errorf("Expected no active timers after stop, got %v", activeTimers)
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
