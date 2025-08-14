# Test Utilities Package

This package provides comprehensive testing utilities for the MAEP distributed key-value store engine. It includes utilities for generating test data, creating test nodes and clusters, and running complex test scenarios.

## Features

- **Operation Generation**: Generate single operations, bulk operations, and mixed operation types
- **Test Nodes**: Easy-to-use test node wrappers with helper methods
- **Test Clusters**: Multi-node testing with synchronization utilities
- **Test Scenarios**: Predefined and customizable test scenarios
- **Error Handling**: Built-in timeout and error handling for robust tests
- **Benchmarking**: Utilities for performance testing

## Quick Start

### Basic Usage

```go
func TestBasicExample(t *testing.T) {
    // Create a test node
    testNode := testutils.NewTestNode(t)
    defer testNode.Stop()
    
    testNode.Start()
    
    // Generate and apply operations
    ops := testutils.GenerateBulkOps(10, "example", engine.OpPut)
    testNode.ApplyOps(ops)
    
    // Assert expected state
    testNode.AssertOpCount(10)
}
```

### Multi-Node Testing

```go
func TestMultiNodeExample(t *testing.T) {
    // Create a 3-node cluster
    cluster := testutils.NewTestCluster(t, 3)
    defer cluster.Stop()
    
    cluster.Start()
    
    // Apply different operations to different nodes
    ops1 := testutils.GenerateBulkOps(5, "node1", engine.OpPut)
    ops2 := testutils.GenerateBulkOps(5, "node2", engine.OpPut)
    
    cluster.ApplyOpsToNode(0, ops1)
    cluster.ApplyOpsToNode(1, ops2)
    
    // Verify all nodes have expected operation counts
    cluster.AssertAllNodesHaveOpCount(5)
}
```

## Operation Generation

### Single Operations

```go
// Basic operation
op := testutils.GenerateOp("key", "value", engine.OpPut)

// Operation with specific actor
actor := testutils.GenerateRandomActorID()
op := testutils.GenerateOpWithActor("key", "value", engine.OpPut, actor)

// Operation with specific HLC timestamp
op := testutils.GenerateOpWithHLC("key", "value", engine.OpPut, 1234567890, 12345)
```

### Bulk Operations

```go
// Generate 100 PUT operations with sequential keys
ops := testutils.GenerateBulkOps(100, "bulk", engine.OpPut)

// Generate operations with different actors
actors := []engine.ActorID{
    testutils.GenerateRandomActorID(),
    testutils.GenerateRandomActorID(),
}
ops := testutils.GenerateBulkOpsWithActors(50, "multi-actor", engine.OpPut, actors)

// Generate mixed PUT/DELETE operations (70% PUT, 30% DELETE)
ops := testutils.GenerateMixedOps(100, "mixed", 0.7)
```

## Test Nodes

### Creating Test Nodes

```go
// Default options
testNode := testutils.NewTestNode(t)

// Custom options
depth := uint8(6)
fanout := uint32(8)
options := engine.NodeOptions{
    MerkleDepth:  &depth,
    MerkleFanout: &fanout,
}
testNode := testutils.NewTestNodeWithOptions(t, options)
```

### Node Operations

```go
testNode := testutils.NewTestNode(t)
defer testNode.Stop()

testNode.Start()

// Apply single operation
op := testutils.GenerateOp("key", "value", engine.OpPut)
testNode.ApplyOp(op)

// Apply multiple operations
ops := testutils.GenerateBulkOps(10, "bulk", engine.OpPut)
testNode.ApplyOps(ops)

// Apply operations concurrently
testNode.ApplyOpsConcurrently(ops)

// Wait for specific operation count
err := testNode.WaitForOpCount(10, 5*time.Second)
if err != nil {
    t.Fatalf("Timeout: %v", err)
}

// Assertions
testNode.AssertOpCount(10)
testNode.AssertOpExists(op)
```

## Test Clusters

### Creating Clusters

```go
// Default options
cluster := testutils.NewTestCluster(t, 3)

// Custom options
options := engine.NodeOptions{
    MerkleDepth:  &depth,
    MerkleFanout: &fanout,
}
cluster := testutils.NewTestClusterWithOptions(t, 3, options)
```

### Cluster Operations

```go
cluster := testutils.NewTestCluster(t, 3)
defer cluster.Stop()

cluster.Start()

// Apply operations to specific nodes
ops1 := testutils.GenerateBulkOps(5, "node1", engine.OpPut)
ops2 := testutils.GenerateBulkOps(5, "node2", engine.OpPut)
cluster.ApplyOpsToNode(0, ops1)
cluster.ApplyOpsToNode(1, ops2)

// Apply operations to all nodes
sharedOp := testutils.GenerateOp("shared", "value", engine.OpPut)
cluster.ApplyOpToAll(sharedOp)

// Wait for all nodes to reach expected state
err := cluster.WaitForAllNodesToHaveOpCount(6, 5*time.Second)

// Assertions
cluster.AssertAllNodesHaveOpCount(6)
cluster.AssertAllNodesHaveSameRoot()
```

## Test Scenarios

### Predefined Scenarios

```go
// Run predefined scenarios
testutils.RunTestScenario(t, testutils.SingleNodeBasicScenario, 1)
testutils.RunTestScenario(t, testutils.MultiNodeSyncScenario, 2)
testutils.RunTestScenario(t, testutils.ConcurrentOpsScenario, 1)
```

### Custom Scenarios

```go
customScenario := testutils.TestScenario{
    Name: "CustomScenario",
    Setup: func(cluster *testutils.TestCluster) {
        // Setup phase
        t.Log("Setting up custom scenario")
    },
    Execute: func(cluster *testutils.TestCluster) {
        // Execute phase
        ops := testutils.GenerateBulkOps(10, "custom", engine.OpPut)
        cluster.ApplyOpsToNode(0, ops)
    },
    Verify: func(cluster *testutils.TestCluster) {
        // Verify phase
        cluster.Nodes[0].AssertOpCount(10)
    },
    Cleanup: func(cluster *testutils.TestCluster) {
        // Cleanup phase
        t.Log("Cleaning up custom scenario")
    },
}

testutils.RunTestScenario(t, customScenario, 2)
```

## Error Handling

The test utilities include built-in error handling and timeout mechanisms:

```go
// Wait with timeout
err := testNode.WaitForOpCount(100, 100*time.Millisecond)
if err != nil {
    t.Logf("Expected timeout: %v", err)
}

// Error handling in scenarios
if err := cluster.WaitForAllNodesToHaveOpCount(10, 5*time.Second); err != nil {
    t.Fatalf("Cluster synchronization failed: %v", err)
}
```

## Benchmarking

```go
func BenchmarkBulkOperations(b *testing.B) {
    // Create test node manually for benchmark
    bus := eventbus.NewEventBus()
    options := engine.DefaultNodeOptions()
    node := engine.NewNode(bus, &options)
    
    node.Start(bus)
    
    // Pre-generate operations
    ops := testutils.GenerateBulkOps(1000, "benchmark", engine.OpPut)
    
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
        node.Start(bus)
    }
}
```

## Best Practices

1. **Always use defer cleanup**: Use `defer testNode.Stop()` or `defer cluster.Stop()` to ensure proper cleanup
2. **Use timeouts**: Always specify reasonable timeouts for wait operations
3. **Test different scenarios**: Use the predefined scenarios and create custom ones for comprehensive testing
4. **Test error conditions**: Include tests for timeout scenarios and error handling
5. **Use assertions**: Use the provided assertion methods for consistent error messages
6. **Benchmark performance**: Use the benchmarking utilities to track performance regressions

## Migration from Old Tests

If you have existing tests using the old `generateOp` function, you can easily migrate:

```go
// Old way
op := generateOp("key", "value", OpPut)

// New way
op := testutils.GenerateOp("key", "value", engine.OpPut)
```

The new utilities provide much more functionality while maintaining backward compatibility for basic operation generation.
