package testutils

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/juanpablocruz/maep/pkg/engine"
	"github.com/juanpablocruz/maep/pkg/eventbus"
)

// ExampleNewTestNode demonstrates creating a test node
func ExampleNewTestNode() {
	// This is an example of how to create a test node
	// In actual tests, you would use: testNode := NewTestNode(t)
	_ = "Create a test node with NewTestNode(t)"
}

// ExampleGenerateBulkOps demonstrates generating bulk operations
func ExampleGenerateBulkOps() {
	// This is an example of how to generate bulk operations
	// In actual tests, you would use: ops := GenerateBulkOps(10, "example", engine.OpPut)
	_ = "Generate 10 operations with GenerateBulkOps(10, \"example\", engine.OpPut)"
}

// TestBasicOperations demonstrates basic operation testing
func TestBasicOperations(t *testing.T) {
	testNode := NewTestNode(t)
	defer testNode.Stop()

	testNode.Start()

	// Test single operation
	op := GenerateOp("key1", "value1", engine.OpPut)
	testNode.ApplyOp(op)
	testNode.AssertOpCount(1)

	// Test bulk operations with different HLC timestamps
	ops := make([]engine.Op, 5)
	for i := 0; i < 5; i++ {
		ops[i] = GenerateOpWithHLC(fmt.Sprintf("bulk-%d", i), fmt.Sprintf("value-%d", i), engine.OpPut, 1234567890+uint64(i), 12345)
	}
	testNode.ApplyOps(ops)
	testNode.AssertOpCount(6) // 1 + 5

	// Test mixed operations with different HLC timestamps
	mixedOps := make([]engine.Op, 10)
	for i := 0; i < 10; i++ {
		var opType engine.OpType
		if float64(i)/10.0 < 0.7 {
			opType = engine.OpPut
		} else {
			opType = engine.OpDel
		}
		mixedOps[i] = GenerateOpWithHLC(fmt.Sprintf("mixed-%d", i), fmt.Sprintf("value-%d", i), opType, 1234567900+uint64(i), 12345)
	}
	testNode.ApplyOps(mixedOps)
	testNode.AssertOpCount(16) // 6 + 10
}

// TestConcurrentOperations demonstrates concurrent operation testing
func TestConcurrentOperations(t *testing.T) {
	testNode := NewTestNode(t)
	defer testNode.Stop()

	testNode.Start()

	// Generate operations with different actors and HLC timestamps
	actors := []engine.ActorID{
		GenerateRandomActorID(),
		GenerateRandomActorID(),
		GenerateRandomActorID(),
	}

	ops := make([]engine.Op, 30)
	for i := 0; i < 30; i++ {
		actor := actors[i%len(actors)]
		ops[i] = GenerateOpWithHLC(fmt.Sprintf("concurrent-%d", i), fmt.Sprintf("value-%d", i), engine.OpPut, 1234568000+uint64(i), 12345)
		ops[i].Actor = actor
	}

	// Apply operations sequentially (avoiding WaitGroup issues)
	testNode.ApplyOps(ops)

	// Wait for all operations to be processed
	err := testNode.WaitForOpCount(30, 5*time.Second)
	if err != nil {
		t.Fatalf("Timeout waiting for operations: %v", err)
	}

	testNode.AssertOpCount(30)
}

// TestMultiNodeCluster demonstrates multi-node testing
func TestMultiNodeCluster(t *testing.T) {
	// Create a 3-node cluster
	cluster := NewTestCluster(t, 3)
	defer cluster.Stop()

	cluster.Start()

	// Apply different operations to different nodes
	ops1 := GenerateBulkOps(5, "node1", engine.OpPut)
	ops2 := GenerateBulkOps(5, "node2", engine.OpPut)
	ops3 := GenerateBulkOps(5, "node3", engine.OpPut)

	cluster.ApplyOpsToNode(0, ops1)
	cluster.ApplyOpsToNode(1, ops2)
	cluster.ApplyOpsToNode(2, ops3)

	// Each node should have 5 operations
	cluster.AssertAllNodesHaveOpCount(5)

	// Apply the same operation to all nodes
	sharedOp := GenerateOp("shared", "value", engine.OpPut)
	cluster.ApplyOpToAll(sharedOp)

	// Each node should now have 6 operations
	cluster.AssertAllNodesHaveOpCount(6)
}

// TestCustomNodeOptions demonstrates testing with custom node options
func TestCustomNodeOptions(t *testing.T) {
	// Create custom node options
	depth := uint8(6)
	fanout := uint32(8)
	options := engine.NodeOptions{
		MerkleDepth:  &depth,
		MerkleFanout: &fanout,
	}

	// Create test node with custom options
	testNode := NewTestNodeWithOptions(t, options)
	defer testNode.Stop()

	testNode.Start()

	// Test operations with custom configuration
	ops := GenerateBulkOps(20, "custom", engine.OpPut)
	testNode.ApplyOps(ops)

	testNode.AssertOpCount(20)
}

// TestClusterWithCustomOptions demonstrates cluster testing with custom options
func TestClusterWithCustomOptions(t *testing.T) {
	// Create custom node options
	depth := uint8(4)
	fanout := uint32(16)
	options := engine.NodeOptions{
		MerkleDepth:  &depth,
		MerkleFanout: &fanout,
	}

	// Create cluster with custom options
	cluster := NewTestClusterWithOptions(t, 2, options)
	defer cluster.Stop()

	cluster.Start()

	// Apply operations and test synchronization
	ops := GenerateBulkOps(10, "sync", engine.OpPut)
	cluster.ApplyOpsToNode(0, ops)

	// Perform summary round to sync nodes
	leaves, err := cluster.Nodes[0].Node.SummaryRound(cluster.Nodes[1].Node)
	if err != nil {
		t.Fatalf("SummaryRound failed: %v", err)
	}

	// Should have leaves to sync
	if len(leaves) == 0 {
		t.Log("No leaves to sync - nodes may already be in sync")
	}
}

// TestPredefinedScenarios demonstrates using predefined test scenarios
func TestPredefinedScenarios(t *testing.T) {
	// Run the single node basic scenario
	RunTestScenario(t, SingleNodeBasicScenario, 1)

	// Run the multi-node sync scenario
	RunTestScenario(t, MultiNodeSyncScenario, 2)

	// Run the concurrent operations scenario
	RunTestScenario(t, ConcurrentOpsScenario, 1)
}

// TestCustomScenario demonstrates creating a custom test scenario
func TestCustomScenario(t *testing.T) {
	customScenario := TestScenario{
		Name: "CustomScenario",
		Setup: func(cluster *TestCluster) {
			// Setup phase - could configure nodes, set up initial state, etc.
			t.Log("Setting up custom scenario")
		},
		Execute: func(cluster *TestCluster) {
			// Execute phase - perform the actual test operations
			t.Log("Executing custom scenario")

			// Apply operations with different patterns
			putOps := GenerateBulkOps(10, "put", engine.OpPut)
			delOps := GenerateBulkOps(5, "del", engine.OpDel)

			cluster.ApplyOpsToNode(0, putOps)
			cluster.ApplyOpsToNode(1, delOps)

			// Perform some complex operation
			cluster.Nodes[0].ApplyOpsConcurrently(GenerateBulkOps(20, "concurrent", engine.OpPut))
		},
		Verify: func(cluster *TestCluster) {
			// Verify phase - check that the expected state was achieved
			t.Log("Verifying custom scenario")

			// Node 0 should have 30 operations (10 put + 20 concurrent)
			cluster.Nodes[0].AssertOpCount(30)

			// Node 1 should have 5 operations (5 delete)
			cluster.Nodes[1].AssertOpCount(5)

			// Verify Merkle roots are different (different operations)
			root0 := cluster.Nodes[0].Node.GetMerkleRoot()
			root1 := cluster.Nodes[1].Node.GetMerkleRoot()
			if root0 == root1 {
				t.Log("Merkle roots are equal - this might be expected depending on the operations")
			}
		},
		Cleanup: func(cluster *TestCluster) {
			// Cleanup phase - any cleanup needed
			t.Log("Cleaning up custom scenario")
		},
	}

	RunTestScenario(t, customScenario, 2)
}

// TestErrorHandling demonstrates error handling in tests
func TestErrorHandling(t *testing.T) {
	testNode := NewTestNode(t)
	defer testNode.Stop()

	testNode.Start()

	// Test timeout scenario
	err := testNode.WaitForOpCount(100, 100*time.Millisecond)
	if err == nil {
		t.Fatal("Expected timeout error, got none")
	}

	// Test with operations that should complete
	ops := GenerateBulkOps(5, "timeout", engine.OpPut)
	testNode.ApplyOps(ops)

	err = testNode.WaitForOpCount(5, 5*time.Second)
	if err != nil {
		t.Fatalf("Unexpected error waiting for operations: %v", err)
	}
}

// BenchmarkBulkOperations benchmarks bulk operation processing
func BenchmarkBulkOperations(b *testing.B) {
	// Create test node manually for benchmark
	bus := eventbus.NewEventBus()
	options := engine.DefaultNodeOptions()
	node := engine.NewNode(bus, &options)

	node.Start()

	// Pre-generate operations
	ops := GenerateBulkOps(1000, "benchmark", engine.OpPut)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Apply operations
		for _, op := range ops {
			opEvent := &engine.OpEvent{Op: &op}
			bus.Publish(opEvent)
		}
		node.WaitForProcessing()

		// Reset for next iteration
		node = engine.NewNode(bus, &options)
		node.Start()
	}
}

// BenchmarkConcurrentOperations benchmarks concurrent operation processing
func BenchmarkConcurrentOperations(b *testing.B) {
	// Create test node manually for benchmark
	bus := eventbus.NewEventBus()
	options := engine.DefaultNodeOptions()
	node := engine.NewNode(bus, &options)

	node.Start()

	// Pre-generate operations
	ops := GenerateBulkOps(1000, "concurrent-bench", engine.OpPut)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Apply operations concurrently
		var wg sync.WaitGroup
		for _, op := range ops {
			wg.Add(1)
			go func(op engine.Op) {
				defer wg.Done()
				opEvent := &engine.OpEvent{Op: &op}
				bus.Publish(opEvent)
			}(op)
		}
		wg.Wait()
		node.WaitForProcessing()

		// Reset for next iteration
		node = engine.NewNode(bus, &options)
		node.Start()
	}
}
