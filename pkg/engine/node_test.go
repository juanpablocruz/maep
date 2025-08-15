package engine

import (
	"bytes"
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
	node.Start(bus)

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

	if node.ch == nil {
		t.Error("Expected event channel to be initialized")
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
	node.Start(bus)

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
	node.Start(bus)

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

	A.Start(bus)
	B.Start(bus)

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

	A.Start(busA)
	B.Start(busB)

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

	A.Start(busA)
	B.Start(busB)

	// Verify initial state
	require.Equal(t, A.m.root, B.m.root)

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
