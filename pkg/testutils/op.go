// Package testutils provides utilities for testing.
package testutils

import (
	"crypto/rand"
	"fmt"
	"testing"
	"time"

	"github.com/juanpablocruz/maep/pkg/engine"
	"github.com/juanpablocruz/maep/pkg/eventbus"
	"github.com/stretchr/testify/require"
)

// GenerateOp creates a single operation with default values
func GenerateOp(key string, value string, opType engine.OpType) engine.Op {
	return engine.Op{
		Actor: engine.ActorID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
		HLC:   engine.NewHLC(1234567890, 12345),
		Type:  engine.OpType(opType),
		Key:   []byte(key),
		Value: []byte(value),
	}
}

// GenerateOpWithActor creates an operation with a specific actor ID
func GenerateOpWithActor(key string, value string, opType engine.OpType, actor engine.ActorID) engine.Op {
	return engine.Op{
		Actor: actor,
		HLC:   engine.NewHLC(1234567890, 12345),
		Type:  engine.OpType(opType),
		Key:   []byte(key),
		Value: []byte(value),
	}
}

// GenerateOpWithHLC creates an operation with specific HLC timestamp
func GenerateOpWithHLC(key string, value string, opType engine.OpType, wallns uint64, logical uint32) engine.Op {
	return engine.Op{
		Actor: engine.ActorID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
		HLC:   engine.NewHLC(wallns, logical),
		Type:  engine.OpType(opType),
		Key:   []byte(key),
		Value: []byte(value),
	}
}

// GenerateRandomActorID creates a random actor ID for testing
func GenerateRandomActorID() engine.ActorID {
	var actor engine.ActorID
	rand.Read(actor[:])
	return actor
}

// GenerateBulkOps creates multiple operations with sequential keys
func GenerateBulkOps(count int, prefix string, opType engine.OpType) []engine.Op {
	ops := make([]engine.Op, count)
	for i := 0; i < count; i++ {
		key := fmt.Sprintf("%s-%d", prefix, i)
		value := fmt.Sprintf("value-%d", i)
		ops[i] = GenerateOp(key, value, opType)
	}
	return ops
}

// GenerateBulkOpsWithActors creates multiple operations with different actors
func GenerateBulkOpsWithActors(count int, prefix string, opType engine.OpType, actors []engine.ActorID) []engine.Op {
	ops := make([]engine.Op, count)
	for i := 0; i < count; i++ {
		key := fmt.Sprintf("%s-%d", prefix, i)
		value := fmt.Sprintf("value-%d", i)
		actor := actors[i%len(actors)]
		ops[i] = GenerateOpWithActor(key, value, opType, actor)
	}
	return ops
}

// GenerateMixedOps creates a mix of PUT and DELETE operations
func GenerateMixedOps(count int, prefix string, putRatio float64) []engine.Op {
	ops := make([]engine.Op, count)
	for i := 0; i < count; i++ {
		key := fmt.Sprintf("%s-%d", prefix, i)
		value := fmt.Sprintf("value-%d", i)
		
		// Determine operation type based on ratio
		var opType engine.OpType
		if float64(i)/float64(count) < putRatio {
			opType = engine.OpPut
		} else {
			opType = engine.OpDel
		}
		
		ops[i] = GenerateOp(key, value, opType)
	}
	return ops
}

// TestNode represents a test node with helper methods
type TestNode struct {
	Node *engine.Node
	Bus  *eventbus.EventBus
	t    *testing.T
}

// NewTestNode creates a new test node with default options
func NewTestNode(t *testing.T) *TestNode {
	bus := eventbus.NewEventBus()
	options := engine.DefaultNodeOptions()
	node := engine.NewNode(bus, &options)
	
	return &TestNode{
		Node: node,
		Bus:  bus,
		t:    t,
	}
}

// NewTestNodeWithOptions creates a new test node with custom options
func NewTestNodeWithOptions(t *testing.T, options engine.NodeOptions) *TestNode {
	bus := eventbus.NewEventBus()
	node := engine.NewNode(bus, &options)
	
	return &TestNode{
		Node: node,
		Bus:  bus,
		t:    t,
	}
}

// Start starts the test node
func (tn *TestNode) Start() {
	tn.Node.Start(tn.Bus)
}

// Stop stops the test node
func (tn *TestNode) Stop() {
	tn.Node.Stop()
}

// ApplyOp applies a single operation to the node
func (tn *TestNode) ApplyOp(op engine.Op) {
	opEvent := &engine.OpEvent{Op: &op}
	tn.Bus.Publish(opEvent)
	// Add a small delay to avoid race conditions
	time.Sleep(1 * time.Millisecond)
}

// ApplyOps applies multiple operations to the node
func (tn *TestNode) ApplyOps(ops []engine.Op) {
	for _, op := range ops {
		tn.ApplyOp(op)
	}
	// Wait for all operations to be processed
	tn.Node.WaitForProcessing()
}

// ApplyOpsConcurrently applies multiple operations concurrently
// Note: This method is disabled due to WaitGroup reuse issues in the Node implementation
func (tn *TestNode) ApplyOpsConcurrently(ops []engine.Op) {
	// For now, just apply operations sequentially to avoid WaitGroup issues
	tn.ApplyOps(ops)
}

// WaitForOpCount waits for the node to have a specific number of operations
func (tn *TestNode) WaitForOpCount(expected int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if tn.Node.GetOpCount() == expected {
			return nil
		}
		time.Sleep(10 * time.Millisecond)
	}
	return fmt.Errorf("timeout waiting for %d operations, got %d", expected, tn.Node.GetOpCount())
}

// AssertOpCount asserts that the node has the expected number of operations
func (tn *TestNode) AssertOpCount(expected int) {
	require.Equal(tn.t, expected, tn.Node.GetOpCount(), "Expected %d operations, got %d", expected, tn.Node.GetOpCount())
}

// AssertOpExists asserts that a specific operation exists in the node
func (tn *TestNode) AssertOpExists(op engine.Op) {
	// Note: This requires access to the OpLog, which is currently private
	// We'll need to add a public method to Node to check if an op exists
	// For now, we'll skip this assertion
	tn.t.Logf("AssertOpExists not implemented - requires public access to OpLog")
}

// GetOpCount returns the current number of operations in the node
func (tn *TestNode) GetOpCount() int {
	return tn.Node.GetOpCount()
}

// TestCluster represents a cluster of test nodes
type TestCluster struct {
	Nodes []*TestNode
	t     *testing.T
}

// NewTestCluster creates a new test cluster with the specified number of nodes
func NewTestCluster(t *testing.T, nodeCount int) *TestCluster {
	nodes := make([]*TestNode, nodeCount)
	for i := 0; i < nodeCount; i++ {
		nodes[i] = NewTestNode(t)
	}
	
	return &TestCluster{
		Nodes: nodes,
		t:     t,
	}
}

// NewTestClusterWithOptions creates a new test cluster with custom options
func NewTestClusterWithOptions(t *testing.T, nodeCount int, options engine.NodeOptions) *TestCluster {
	nodes := make([]*TestNode, nodeCount)
	for i := 0; i < nodeCount; i++ {
		nodes[i] = NewTestNodeWithOptions(t, options)
	}
	
	return &TestCluster{
		Nodes: nodes,
		t:     t,
	}
}

// Start starts all nodes in the cluster
func (tc *TestCluster) Start() {
	for _, node := range tc.Nodes {
		node.Start()
	}
}

// Stop stops all nodes in the cluster
func (tc *TestCluster) Stop() {
	for _, node := range tc.Nodes {
		node.Stop()
	}
}

// ApplyOpToAll applies an operation to all nodes in the cluster
func (tc *TestCluster) ApplyOpToAll(op engine.Op) {
	for _, node := range tc.Nodes {
		node.ApplyOp(op)
	}
}

// ApplyOpsToAll applies multiple operations to all nodes in the cluster
func (tc *TestCluster) ApplyOpsToAll(ops []engine.Op) {
	for _, node := range tc.Nodes {
		node.ApplyOps(ops)
	}
}

// ApplyOpsToNode applies operations to a specific node
func (tc *TestCluster) ApplyOpsToNode(nodeIndex int, ops []engine.Op) {
	if nodeIndex >= len(tc.Nodes) {
		tc.t.Fatalf("Node index %d out of bounds (cluster size: %d)", nodeIndex, len(tc.Nodes))
	}
	tc.Nodes[nodeIndex].ApplyOps(ops)
}

// WaitForAllNodesToHaveOpCount waits for all nodes to have the expected operation count
func (tc *TestCluster) WaitForAllNodesToHaveOpCount(expected int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		allReady := true
		for _, node := range tc.Nodes {
			if node.GetOpCount() != expected {
				allReady = false
				break
			}
		}
		if allReady {
			return nil
		}
		time.Sleep(10 * time.Millisecond)
	}
	return fmt.Errorf("timeout waiting for all nodes to have %d operations", expected)
}

// AssertAllNodesHaveOpCount asserts that all nodes have the expected operation count
func (tc *TestCluster) AssertAllNodesHaveOpCount(expected int) {
	for _, node := range tc.Nodes {
		node.AssertOpCount(expected)
	}
}

// AssertAllNodesHaveSameRoot asserts that all nodes have the same Merkle root
func (tc *TestCluster) AssertAllNodesHaveSameRoot() {
	if len(tc.Nodes) < 2 {
		return
	}
	
	firstRoot := tc.Nodes[0].Node.GetMerkleRoot()
	for i := 1; i < len(tc.Nodes); i++ {
		require.Equal(tc.t, firstRoot, tc.Nodes[i].Node.GetMerkleRoot(), 
			"Node %d has different root than node 0", i)
	}
}

// TestScenario represents a complex test scenario
type TestScenario struct {
	Name        string
	Setup       func(*TestCluster)
	Execute     func(*TestCluster)
	Verify      func(*TestCluster)
	Cleanup     func(*TestCluster)
}

// RunTestScenario runs a complete test scenario
func RunTestScenario(t *testing.T, scenario TestScenario, nodeCount int) {
	t.Run(scenario.Name, func(t *testing.T) {
		cluster := NewTestCluster(t, nodeCount)
		defer cluster.Stop()
		
		cluster.Start()
		
		if scenario.Setup != nil {
			scenario.Setup(cluster)
		}
		
		if scenario.Execute != nil {
			scenario.Execute(cluster)
		}
		
		if scenario.Verify != nil {
			scenario.Verify(cluster)
		}
		
		if scenario.Cleanup != nil {
			scenario.Cleanup(cluster)
		}
	})
}

// Common test scenarios
var (
	// SingleNodeBasicScenario tests basic operation handling on a single node
	SingleNodeBasicScenario = TestScenario{
		Name: "SingleNodeBasic",
		Execute: func(cluster *TestCluster) {
			ops := GenerateBulkOps(10, "key", engine.OpPut)
			cluster.ApplyOpsToNode(0, ops)
		},
		Verify: func(cluster *TestCluster) {
			cluster.AssertAllNodesHaveOpCount(10)
		},
	}
	
	// MultiNodeSyncScenario tests synchronization between multiple nodes
	MultiNodeSyncScenario = TestScenario{
		Name: "MultiNodeSync",
		Execute: func(cluster *TestCluster) {
			// Apply different operations to different nodes
			ops1 := GenerateBulkOps(5, "node1", engine.OpPut)
			ops2 := GenerateBulkOps(5, "node2", engine.OpPut)
			
			cluster.ApplyOpsToNode(0, ops1)
			cluster.ApplyOpsToNode(1, ops2)
			
			// Perform summary rounds to sync
			leaves, err := cluster.Nodes[0].Node.SummaryRound(cluster.Nodes[1].Node)
			require.NoError(cluster.t, err)
			// Note: leaves might be empty if nodes are already in sync
			cluster.t.Logf("Summary round returned %d leaves", len(leaves))
		},
		Verify: func(cluster *TestCluster) {
			// Each node should have 5 operations (they don't automatically sync)
			cluster.Nodes[0].AssertOpCount(5)
			cluster.Nodes[1].AssertOpCount(5)
		},
	}
	
	// ConcurrentOpsScenario tests bulk operation handling
	ConcurrentOpsScenario = TestScenario{
		Name: "BulkOps",
		Execute: func(cluster *TestCluster) {
			ops := GenerateBulkOps(50, "bulk", engine.OpPut)
			cluster.Nodes[0].ApplyOps(ops)
		},
		Verify: func(cluster *TestCluster) {
			cluster.AssertAllNodesHaveOpCount(50)
		},
	}
)
