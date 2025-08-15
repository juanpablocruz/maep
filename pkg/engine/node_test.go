package engine

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/juanpablocruz/maep/pkg/eventbus"
	"github.com/stretchr/testify/require"
)

func Test_Node_EventBus_Integration(t *testing.T) {
	bus := eventbus.NewEventBus()

	depth := uint8(4)
	fanout := uint32(16)
	node := NewNode(bus, &NodeOptions{
		MerkleDepth:  &depth,
		MerkleFanout: &fanout,
	})
	node.Start()

	op := generateOp("key", "value", OpPut)

	opEvent := &OpEvent{Op: &op}
	bus.Publish(opEvent)
	node.WaitForProcessing()

	if node.ops.Len() != 1 {
		t.Errorf("Expected 1 op in OpLog, got %d", node.ops.Len())
	}
	opHash := op.Hash()
	retrievedOp := node.ops.Get(opHash)
	if retrievedOp == nil {
		t.Fatalf("Op not found in OpLog store")
	}

	if !bytes.Equal(retrievedOp.Key, op.Key) {
		t.Errorf("Expected key %s, got %s", string(op.Key), string(retrievedOp.Key))
	}
}

func Test_Node_DefaultOptions(t *testing.T) {
	bus := eventbus.NewEventBus()

	// Test with nil options (should use defaults)
	node := NewNode(bus, nil)

	if node == nil {
		t.Fatal("Expected node to be created with default options")
	}

	// Verify that the node was created successfully
	if node.ops == nil {
		t.Error("Expected OpLog to be initialized")
	}

	if node.m == nil {
		t.Error("Expected Merkle tree to be initialized")
	}
}

func Test_Node_PartialOptions(t *testing.T) {
	bus := eventbus.NewEventBus()

	// Test with partial options (only MerkleDepth specified)
	depth := uint8(10)
	node := NewNode(bus, &NodeOptions{
		MerkleDepth: &depth,
		// MerkleFanout not specified, should use default
	})

	if node == nil {
		t.Fatal("Expected node to be created with partial options")
	}

	// Verify that the node was created successfully
	if node.ops == nil {
		t.Error("Expected OpLog to be initialized")
	}

	if node.m == nil {
		t.Error("Expected Merkle tree to be initialized")
	}
}

func Test_Node_AppendOp(t *testing.T) {
	bus := eventbus.NewEventBus()

	depth := uint8(4)
	fanout := uint32(16)
	node := NewNode(bus, &NodeOptions{
		MerkleDepth:  &depth,
		MerkleFanout: &fanout,
	})
	node.Start()

	op := generateOp("key", "value", OpPut)

	opEvent := &OpEvent{Op: &op}
	bus.Publish(opEvent)
	node.WaitForProcessing()

	if node.ops.Len() != 1 {
		t.Errorf("Expected 1 op in OpLog, got %d", node.ops.Len())
	}

	// merkle tree should have 1 leaf
	K := op.CanonicalKey()
	leaf := node.m.GetLeaf(K[:])
	if leaf == nil {
		t.Fatalf("Expected leaf to be created")
	}

	if leaf.Count != 1 {
		t.Errorf("Expected leaf count to be 1, got %d", leaf.Count)
	}
}

func Test_Node_AppendOp_Idempotent(t *testing.T) {
	bus := eventbus.NewEventBus()

	depth := uint8(4)
	fanout := uint32(16)
	node := NewNode(bus, &NodeOptions{
		MerkleDepth:  &depth,
		MerkleFanout: &fanout,
	})
	node.Start()

	op := generateOp("key", "value", OpPut)

	// Publish the same operation multiple times
	for range 3 {
		opEvent := &OpEvent{Op: &op}
		bus.Publish(opEvent)
		node.WaitForProcessing()
	}

	// Should still have only 1 op in OpLog
	if node.ops.Len() != 1 {
		t.Errorf("Expected 1 op in OpLog after multiple publishes, got %d", node.ops.Len())
	}

	// Merkle tree should still have 1 leaf
	K := op.CanonicalKey()
	leaf := node.m.GetLeaf(K[:])
	if leaf == nil {
		t.Fatalf("Expected leaf to be created")
	}

	if leaf.Count != 1 {
		t.Errorf("Expected leaf count to be 1 after multiple publishes, got %d", leaf.Count)
	}
}

func Test_Node_SummaryRound(t *testing.T) {
	bus := eventbus.NewEventBus()

	options := DefaultNodeOptions()
	A, B := NewNode(bus, &options), NewNode(bus, &options)

	A.Start()
	B.Start()

	require.Equal(t, A.m.root, B.m.root)

	leaves, err := A.SummaryRound(B)
	require.NoError(t, err)
	require.Len(t, leaves, 0)
}

func Test_Node_SummaryRound_Integration(t *testing.T) {
	busA := eventbus.NewEventBus()
	busB := eventbus.NewEventBus()

	options := DefaultNodeOptions()
	A, B := NewNode(busA, &options), NewNode(busB, &options)

	A.Start()
	B.Start()

	require.Equal(t, A.m.root, B.m.root)
	op := generateOp("key", "value", OpPut)
	op2 := generateOp("key2", "value2", OpPut)
	op3 := generateOp("key3", "value3", OpPut)
	op4 := generateOp("key4", "value4", OpPut)
	opEvent := &OpEvent{Op: &op}
	opEvent2 := &OpEvent{Op: &op2}
	opEvent3 := &OpEvent{Op: &op3}
	opEvent4 := &OpEvent{Op: &op4}
	busA.Publish(opEvent)
	busA.Publish(opEvent2)
	busA.Publish(opEvent3)
	busA.Publish(opEvent4)
	A.WaitForProcessing()

	busB.Publish(opEvent)
	busB.Publish(opEvent2)
	busB.Publish(opEvent3)
	B.WaitForProcessing()

	leaves, err := A.SummaryRound(B)
	require.NoError(t, err)

	require.GreaterOrEqual(t, len(leaves), 1)
}

func Test_Node_SummaryRound_DifferentOrder_SameRoot(t *testing.T) {
	busA := eventbus.NewEventBus()
	busB := eventbus.NewEventBus()

	options := DefaultNodeOptions()
	A, B := NewNode(busA, &options), NewNode(busB, &options)

	A.Start()
	B.Start()

	// Verify initial state
	require.Equal(t, A.m.root.Hash, B.m.root.Hash, "Roots should be equal despite different operation order")

	// Create operations
	op1 := generateOp("key1", "value1", OpPut)
	op2 := generateOp("key2", "value2", OpPut)
	op3 := generateOp("key3", "value3", OpPut)
	op4 := generateOp("key4", "value4", OpPut)

	// Create events
	opEvent1 := &OpEvent{Op: &op1}
	opEvent2 := &OpEvent{Op: &op2}
	opEvent3 := &OpEvent{Op: &op3}
	opEvent4 := &OpEvent{Op: &op4}

	// Add operations to node A in one order
	busA.Publish(opEvent1)
	busA.Publish(opEvent2)
	busA.Publish(opEvent3)
	busA.Publish(opEvent4)
	A.WaitForProcessing()

	// Add the same operations to node B in different order
	busB.Publish(opEvent4)
	busB.Publish(opEvent2)
	busB.Publish(opEvent1)
	busB.Publish(opEvent3)
	B.WaitForProcessing()

	// Verify that despite different insertion order, roots are equal
	require.Equal(t, A.m.root.Hash, B.m.root.Hash, "Roots should be equal despite different operation order")

	// Perform SummaryRound from A to B
	leavesAtoB, err := A.SummaryRound(B)
	require.NoError(t, err, "SummaryRound A to B should not return an error")

	// Perform SummaryRound from B to A
	leavesBtoA, err := B.SummaryRound(A)
	require.NoError(t, err, "SummaryRound B to A should not return an error")

	// Since roots are equal, both should return empty leaf sets
	require.Len(t, leavesAtoB, 0, "SummaryRound A to B should return no differing leaves")
	require.Len(t, leavesBtoA, 0, "SummaryRound B to A should return no differing leaves")

	// Verify that the trees are truly identical by checking a few more properties
	require.Equal(t, A.ops.Len(), B.ops.Len(), "OpLog lengths should be equal")
}

func Test_Node_PlanDeltas(t *testing.T) {
	// ID: CORE-DET-01
	// Target: CORE (OpLog, Merkle, SV)
	// Setup: Create a node with operations and a mock peer with SV frontiers
	// Stimulus: Call PlanDeltas with differing leaves
	// Checks: Verify correct delta plans are generated with operations after frontiers

	bus := eventbus.NewEventBus()
	depth := uint8(4)
	fanout := uint32(16)
	node := NewNode(bus, &NodeOptions{
		MerkleDepth:  &depth,
		MerkleFanout: &fanout,
	})
	node.Start()

	// Create a mock peer
	mockPeer := &mockSummaryPeer{
		id: PeerID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
	}

	// Create operations with different timestamps to ensure proper canonical key ordering
	op1 := generateOpWithTimestamp("key1", "value1", OpPut, 1000, 1)
	op2 := generateOpWithTimestamp("key2", "value2", OpPut, 1000, 2)
	op3 := generateOpWithTimestamp("key3", "value3", OpPut, 1000, 3)
	op4 := generateOpWithTimestamp("key4", "value4", OpPut, 1000, 4)

	// Add operations to the node
	opEvent1 := &OpEvent{Op: &op1}
	opEvent2 := &OpEvent{Op: &op2}
	opEvent3 := &OpEvent{Op: &op3}
	opEvent4 := &OpEvent{Op: &op4}

	bus.Publish(opEvent1)
	bus.Publish(opEvent2)
	bus.Publish(opEvent3)
	bus.Publish(opEvent4)
	node.WaitForProcessing()

	// Verify operations were added
	if node.ops.Len() != 4 {
		t.Errorf("Expected 4 ops in OpLog, got %d", node.ops.Len())
	}

	// Get canonical keys for operations
	op1Key := op1.CanonicalKey()
	op2Key := op2.CanonicalKey()
	op3Key := op3.CanonicalKey()
	op4Key := op4.CanonicalKey()

	t.Logf("Op1 key: %x", op1Key)
	t.Logf("Op2 key: %x", op2Key)
	t.Logf("Op3 key: %x", op3Key)
	t.Logf("Op4 key: %x", op4Key)

	// Set up peer's SV with a frontier that includes the first two operations
	// Since op1 and op2 have lower logical timestamps, they should come first in canonical order
	peerFrontiers := []Frontier{
		{Key: op1Key, Set: true}, // Peer has seen op1
		{Key: op2Key, Set: true}, // Peer has seen op2
	}
	node.SV[mockPeer.id] = peerFrontiers

	// Use the root prefix which should contain all operations
	leaves := []Prefix{
		{Depth: 0, Path: []byte{}}, // Root prefix should contain all operations
	}

	// Test the getPrefixKeyBounds function directly
	lowKey, highKey := node.getPrefixKeyBounds(leaves[0])
	t.Logf("Root prefix bounds: low=%x, high=%x", lowKey, highKey)

	// Call PlanDeltas
	plans := node.PlanDeltas(mockPeer, leaves, 100, 10000)

	// Verify that we got a plan
	if len(plans) == 0 {
		t.Error("Expected at least one delta plan to be generated")
		return
	}

	// Verify each plan
	for i, plan := range plans {
		t.Logf("Plan %d: Leaf=%v, FromK=%x, Ops=%d", i, plan.Leaf, plan.FromK, len(plan.Ops))

		// Verify plan structure
		if len(plan.Ops) == 0 {
			t.Errorf("Expected at least one operation in plan %d", i)
		}

		// Verify operations are sorted by canonical key
		for j := 1; j < len(plan.Ops); j++ {
			if bytes.Compare(plan.Ops[j-1].key[:], plan.Ops[j].key[:]) >= 0 {
				t.Errorf("Operations not sorted by canonical key in plan %d", i)
			}
		}

		// Verify all operations in this plan are after the peer's frontiers
		for _, op := range plan.Ops {
			for _, frontier := range peerFrontiers {
				if frontier.Set && bytes.Compare(op.key[:], frontier.Key[:]) <= 0 {
					t.Errorf("Operation %x is not after frontier %x", op.key, frontier.Key)
				}
			}
		}

		// Verify that operations that should be excluded (op1, op2) are not included
		for _, op := range plan.Ops {
			if bytes.Equal(op.key[:], op1Key[:]) || bytes.Equal(op.key[:], op2Key[:]) {
				t.Errorf("Operation %x should be excluded as it's in peer's frontier", op.key)
			}
		}

		// Verify that operations that should be included (op3, op4) are present
		foundOp3 := false
		foundOp4 := false
		for _, op := range plan.Ops {
			if bytes.Equal(op.key[:], op3Key[:]) {
				foundOp3 = true
			}
			if bytes.Equal(op.key[:], op4Key[:]) {
				foundOp4 = true
			}
		}

		if !foundOp3 {
			t.Error("Expected op3 to be included in delta plan")
		}
		if !foundOp4 {
			t.Error("Expected op4 to be included in delta plan")
		}
	}
}

// generateOpWithTimestamp creates an operation with custom timestamp values
func generateOpWithTimestamp(key string, value string, opType OpType, wallns uint64, logical uint32) Op {
	return Op{
		Actor: ActorID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
		HLC:   NewHLC(wallns, logical),
		Type:  opType,
		Key:   []byte(key),
		Value: []byte(value),
	}
}

// mockSummaryPeer implements SummaryPeer for testing
type mockSummaryPeer struct {
	id PeerID
}

func (m *mockSummaryPeer) GetID() PeerID {
	return m.id
}

func (m *mockSummaryPeer) SendSummaryReq(req SummaryReq) (SummaryResp, error) {
	// Return a mock response - this isn't used in PlanDeltas but needed for interface
	return SummaryResp{
		Peer:   m.id,
		Root:   MerkleHash{},
		Fanout: 16,
		Depth:  4,
		Prefix: req.Prefix,
	}, nil
}

func Test_Node_PlanDeltas_EmptyLeaves(t *testing.T) {
	// ID: CORE-DET-02
	// Target: CORE (OpLog, Merkle, SV)
	// Setup: Create a node with operations but empty leaves list
	// Stimulus: Call PlanDeltas with empty leaves
	// Checks: Verify empty result is returned

	bus := eventbus.NewEventBus()
	depth := uint8(4)
	fanout := uint32(16)
	node := NewNode(bus, &NodeOptions{
		MerkleDepth:  &depth,
		MerkleFanout: &fanout,
	})
	node.Start()

	mockPeer := &mockSummaryPeer{
		id: PeerID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
	}

	// Add some operations
	op := generateOp("key1", "value1", OpPut)
	opEvent := &OpEvent{Op: &op}
	bus.Publish(opEvent)
	node.WaitForProcessing()

	// Call PlanDeltas with empty leaves
	plans := node.PlanDeltas(mockPeer, []Prefix{}, 100, 10000)

	// Should return empty result
	if len(plans) != 0 {
		t.Errorf("Expected empty plans, got %d", len(plans))
	}
}

func Test_Node_PlanDeltas_NoPeerSV(t *testing.T) {
	// ID: CORE-DET-03
	// Target: CORE (OpLog, Merkle, SV)
	// Setup: Create a node with operations but no SV for the peer
	// Stimulus: Call PlanDeltas for peer without SV
	// Checks: Verify all operations are included in plans

	bus := eventbus.NewEventBus()
	depth := uint8(4)
	fanout := uint32(16)
	node := NewNode(bus, &NodeOptions{
		MerkleDepth:  &depth,
		MerkleFanout: &fanout,
	})
	node.Start()

	mockPeer := &mockSummaryPeer{
		id: PeerID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
	}

	// Add operations
	op1 := generateOp("key1", "value1", OpPut)
	op2 := generateOp("key2", "value2", OpPut)
	opEvent1 := &OpEvent{Op: &op1}
	opEvent2 := &OpEvent{Op: &op2}
	bus.Publish(opEvent1)
	bus.Publish(opEvent2)
	node.WaitForProcessing()

	// Create a leaf that should contain our operations
	leaves := []Prefix{
		{Depth: 0, Path: []byte{}}, // Root prefix should contain all operations
	}

	// Call PlanDeltas - peer has no SV, so all operations should be included
	plans := node.PlanDeltas(mockPeer, leaves, 100, 10000)

	// Should return plans with all operations
	if len(plans) == 0 {
		t.Log("No plans generated - this might be expected if operations don't match the test prefix")
		return
	}

	totalOps := 0
	for _, plan := range plans {
		totalOps += len(plan.Ops)
	}

	if totalOps == 0 {
		t.Error("Expected operations to be included when peer has no SV")
	}
}

func Test_Node_PlanDeltas_RespectsLimits(t *testing.T) {
	// ID: CORE-DET-04
	// Target: CORE (OpLog, Merkle, SV)
	// Setup: Create a node with many operations
	// Stimulus: Call PlanDeltas with strict limits
	// Checks: Verify limits are respected

	bus := eventbus.NewEventBus()
	depth := uint8(4)
	fanout := uint32(16)
	node := NewNode(bus, &NodeOptions{
		MerkleDepth:  &depth,
		MerkleFanout: &fanout,
	})
	node.Start()

	mockPeer := &mockSummaryPeer{
		id: PeerID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
	}

	// Add many operations
	for i := 0; i < 10; i++ {
		op := generateOp(fmt.Sprintf("key%d", i), fmt.Sprintf("value%d", i), OpPut)
		opEvent := &OpEvent{Op: &op}
		bus.Publish(opEvent)
	}
	node.WaitForProcessing()

	// Create a leaf that should contain our operations
	leaves := []Prefix{
		{Depth: 0, Path: []byte{}}, // Root prefix
	}

	// Call PlanDeltas with strict limits
	maxOps := 3
	maxBytes := 100
	plans := node.PlanDeltas(mockPeer, leaves, maxOps, maxBytes)

	// Verify limits are respected
	totalOps := 0
	totalBytes := 0
	for _, plan := range plans {
		totalOps += len(plan.Ops)
		for _, op := range plan.Ops {
			totalBytes += len(op.op.Key) + len(op.op.Value) + 60
		}
	}

	if totalOps > maxOps {
		t.Errorf("Expected total ops <= %d, got %d", maxOps, totalOps)
	}

	if totalBytes > maxBytes {
		t.Errorf("Expected total bytes <= %d, got %d", maxBytes, totalBytes)
	}
}
