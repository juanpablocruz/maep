package merkle_test

import (
	"fmt"
	"testing"

	"github.com/juanpablocruz/maep/pkg/engine"
	"github.com/juanpablocruz/maep/pkg/merkle"
)

type TestHasher struct {
	ops   []engine.OpLogEntry
	opLog *engine.OpLog
}

func (th TestHasher) Sort(hs []merkle.OpHash) {
	newOrder := make([]merkle.OpHash, 0)
	for _, o := range th.opLog.GetOrdered() {
		h := merkle.OpHash{}
		oH := o.CanonicalKey()
		copy(h[:], oH[:])
		newOrder = append(newOrder, h)
	}
	copy(hs, newOrder)
}

func NewTestHasher() *TestHasher {
	ops := engine.GenerateOps(6)
	opLog := engine.NewOpLog()
	for _, o := range ops {
		opLog.Append(&o)
	}
	allOpLogs := make([]engine.OpLogEntry, 0)
	for _, entry := range opLog.GetAll() {
		allOpLogs = append(allOpLogs, *entry)
	}

	th := &TestHasher{
		ops:   allOpLogs,
		opLog: opLog,
	}
	return th
}

type OpLogMerkleEntry struct {
	opLog engine.OpLogEntry
	hash  engine.OpCannonicalKey
}

func (ome OpLogMerkleEntry) ComputeHash() merkle.OpHash {
	var hash merkle.OpHash
	copy(hash[:], ome.hash[:])
	return hash
}

func Test_MERKLE_Append_00(t *testing.T) {
	cfg := merkle.Config{
		Fanout:   16,
		MaxDepth: 4,
	}
	th := NewTestHasher()
	cfg.Hasher = th
	m, err := merkle.New(cfg)
	if err != nil {
		t.Errorf("error creating merkle tree: %v", err)
	}

	ome := OpLogMerkleEntry{}
	ome.opLog = th.ops[1]
	ome.hash = ome.opLog.CanonicalKey()

	err = m.AppendOp(ome)
	if err != nil {
		t.Errorf("error appending: %v", err)
	}

}

func Test_MERKLE_Append_Idempotency_01(t *testing.T) {
	cfg := merkle.Config{
		Fanout:   16,
		MaxDepth: 4,
	}
	th := NewTestHasher()
	cfg.Hasher = th
	m, err := merkle.New(cfg)
	if err != nil {
		t.Fatalf("error creating merkle tree: %v", err)
	}

	// Create an operation entry
	ome := OpLogMerkleEntry{}
	ome.opLog = th.ops[0]
	ome.hash = ome.opLog.CanonicalKey()

	// First append
	err = m.AppendOp(ome)
	if err != nil {
		t.Fatalf("error on first append: %v", err)
	}

	// Verify operation is in tree
	if !m.ContainsOp(ome) {
		t.Fatalf("operation should be in tree after first append")
	}

	// Capture initial state
	initialSnapshot := m.Snapshot()

	// Second append (should be idempotent)
	err = m.AppendOp(ome)
	if err != nil {
		t.Fatalf("error on second append: %v", err)
	}

	// Verify operation is still in tree
	if !m.ContainsOp(ome) {
		t.Fatalf("operation should still be in tree after second append")
	}

	// Verify tree state hasn't changed (idempotency)
	finalSnapshot := m.Snapshot()
	if initialSnapshot.Root() != finalSnapshot.Root() {
		t.Errorf("tree root changed after idempotent append: initial=%x, final=%x",
			initialSnapshot.Root(), finalSnapshot.Root())
	}
}

func Test_MERKLE_ContainsOp_02(t *testing.T) {
	cfg := merkle.Config{
		Fanout:   16,
		MaxDepth: 4,
	}
	th := NewTestHasher()
	cfg.Hasher = th
	m, err := merkle.New(cfg)
	if err != nil {
		t.Fatalf("error creating merkle tree: %v", err)
	}

	// Create an operation entry
	ome := OpLogMerkleEntry{}
	ome.opLog = th.ops[0]
	ome.hash = ome.opLog.CanonicalKey()

	// Operation should not be in tree initially
	if m.ContainsOp(ome) {
		t.Fatalf("operation should not be in tree before append")
	}

	// Append the operation
	err = m.AppendOp(ome)
	if err != nil {
		t.Fatalf("error appending operation: %v", err)
	}

	// Operation should now be in tree
	if !m.ContainsOp(ome) {
		t.Fatalf("operation should be in tree after append")
	}

	// Create a different operation
	ome2 := OpLogMerkleEntry{}
	ome2.opLog = th.ops[1]
	ome2.hash = ome2.opLog.CanonicalKey()

	// Different operation should not be in tree
	if m.ContainsOp(ome2) {
		t.Fatalf("different operation should not be in tree")
	}
}

func Test_MERKLE_Append_Multiple_03(t *testing.T) {
	cfg := merkle.Config{
		Fanout:   16,
		MaxDepth: 4,
	}
	th := NewTestHasher()
	cfg.Hasher = th
	m, err := merkle.New(cfg)
	if err != nil {
		t.Fatalf("error creating merkle tree: %v", err)
	}

	// Append multiple operations
	for i := range 3 {
		ome := OpLogMerkleEntry{}
		ome.opLog = th.ops[i]
		ome.hash = ome.opLog.CanonicalKey()

		err = m.AppendOp(ome)
		if err != nil {
			t.Fatalf("error appending operation %d: %v", i, err)
		}

		// Verify operation is in tree
		if !m.ContainsOp(ome) {
			t.Fatalf("operation %d should be in tree after append", i)
		}
	}

	// Verify all operations are still in tree
	for i := range 3 {
		ome := OpLogMerkleEntry{}
		ome.opLog = th.ops[i]
		ome.hash = ome.opLog.CanonicalKey()

		if !m.ContainsOp(ome) {
			t.Fatalf("operation %d should still be in tree", i)
		}
	}

	// Verify tree stats
	stats := m.Stats()
	if stats == nil {
		t.Fatalf("stats should not be nil")
	}
}

func Test_MERKLE_Append_ErrorHandling_04(t *testing.T) {
	cfg := merkle.Config{
		Fanout:   16,
		MaxDepth: 4,
	}
	th := NewTestHasher()
	cfg.Hasher = th
	m, err := merkle.New(cfg)
	if err != nil {
		t.Fatalf("error creating merkle tree: %v", err)
	}

	// Test with nil entry
	err = m.AppendOp(nil)
	if err == nil {
		t.Fatalf("should return error for nil entry")
	}

	// Test with invalid max depth
	cfgInvalid := merkle.Config{
		Fanout:   16,
		MaxDepth: -1, // Invalid depth
	}
	mInvalid, err := merkle.New(cfgInvalid)
	if err == nil {
		t.Fatalf("should return error for invalid max depth")
	}
	if mInvalid != nil {
		t.Fatalf("should return nil merkle tree for invalid config")
	}
}

func Test_MERKLE_Snapshot_05(t *testing.T) {
	cfg := merkle.Config{
		Fanout:   16,
		MaxDepth: 4,
	}
	th := NewTestHasher()
	cfg.Hasher = th
	m, err := merkle.New(cfg)
	if err != nil {
		t.Fatalf("error creating merkle tree: %v", err)
	}

	// Take snapshot of empty tree
	emptySnapshot := m.Snapshot()
	if emptySnapshot == nil {
		t.Fatalf("snapshot should not be nil")
	}

	if emptySnapshot.Fanout() != 16 {
		t.Errorf("expected fanout 16, got %d", emptySnapshot.Fanout())
	}

	if emptySnapshot.MaxDepth() != 4 {
		t.Errorf("expected max depth 4, got %d", emptySnapshot.MaxDepth())
	}

	// Append an operation
	ome := OpLogMerkleEntry{}
	ome.opLog = th.ops[0]
	ome.hash = ome.opLog.CanonicalKey()

	err = m.AppendOp(ome)
	if err != nil {
		t.Fatalf("error appending operation: %v", err)
	}

	// Take snapshot of tree with operation
	filledSnapshot := m.Snapshot()
	if filledSnapshot == nil {
		t.Fatalf("snapshot should not be nil")
	}

	// Root should be different between empty and filled snapshots
	if emptySnapshot.Root() == filledSnapshot.Root() {
		t.Errorf("root should be different between empty and filled snapshots")
	}

	// Epoch should be different (time-based)
	if emptySnapshot.Epoch() == filledSnapshot.Epoch() {
		t.Errorf("epoch should be different between snapshots")
	}
}

func Test_MERKLE_TreeStructure_06(t *testing.T) {
	cfg := merkle.Config{
		Fanout:   16,
		MaxDepth: 2, // Small depth for easier testing
	}
	th := NewTestHasher()
	cfg.Hasher = th
	m, err := merkle.New(cfg)
	if err != nil {
		t.Fatalf("error creating merkle tree: %v", err)
	}

	// Append operations with different hashes to test tree structure
	operations := []OpLogMerkleEntry{}
	for i := range 3 {
		ome := OpLogMerkleEntry{}
		ome.opLog = th.ops[i]
		ome.hash = ome.opLog.CanonicalKey()
		operations = append(operations, ome)

		err = m.AppendOp(ome)
		if err != nil {
			t.Fatalf("error appending operation %d: %v", i, err)
		}
	}

	// Verify all operations are in tree
	for i, ome := range operations {
		if !m.ContainsOp(ome) {
			t.Fatalf("operation %d should be in tree", i)
		}
	}

	// Test that tree maintains consistency after multiple operations
	snapshot1 := m.Snapshot()

	// Append the same operations again (should be idempotent)
	for i, ome := range operations {
		err = m.AppendOp(ome)
		if err != nil {
			t.Fatalf("error on idempotent append of operation %d: %v", i, err)
		}
	}

	snapshot2 := m.Snapshot()

	// Snapshots should be identical after idempotent operations
	if snapshot1.Root() != snapshot2.Root() {
		t.Errorf("tree root changed after idempotent operations: %x vs %x",
			snapshot1.Root(), snapshot2.Root())
	}

	// Test tree close
	err = m.Close()
	if err != nil {
		t.Errorf("error closing tree: %v", err)
	}
}

func Test_MERKLE_Proof_Verify_07(t *testing.T) {

	cfg := merkle.Config{
		Fanout:   16,
		MaxDepth: 2, // Small depth for easier testing
	}
	th := NewTestHasher()
	cfg.Hasher = th
	m, err := merkle.New(cfg)
	if err != nil {
		t.Fatalf("error creating merkle tree: %v", err)
	}

	// Append operations with different hashes to test tree structure
	operations := []OpLogMerkleEntry{}
	for i := range 3 {
		ome := OpLogMerkleEntry{}
		ome.opLog = th.ops[i]
		ome.hash = ome.opLog.CanonicalKey()
		operations = append(operations, ome)

		err = m.AppendOp(ome)
		if err != nil {
			t.Fatalf("error appending operation %d: %v", i, err)
		}
	}

	// Verify all operations are in tree
	for i, ome := range operations {
		if !m.ContainsOp(ome) {
			t.Fatalf("operation %d should be in tree", i)
		}
	}

	// Test that tree maintains consistency after multiple operations
	snapshot1 := m.Snapshot()

	fmt.Printf("Snapshot1: %x\n", snapshot1.Root())

	proof, err := snapshot1.ProofForKey(merkle.OpHash(operations[0].opLog.CanonicalKey()))
	if err != nil {
		t.Fatalf("error computing proof for key: %v", err)
	}

	if !merkle.VerifyOpProof(snapshot1.Root(), merkle.OpHash(operations[0].opLog.CanonicalKey()), 2, proof, th) {
		t.Errorf("proof verification failed")
	}
}

func Test_MERKLE_Descent_08(t *testing.T) {
	cfg := merkle.Config{
		Fanout:   16,
		MaxDepth: 2, // Small depth for easier testing
	}
	th := NewTestHasher()
	cfg.Hasher = th
	m, err := merkle.New(cfg)
	if err != nil {
		t.Fatalf("error creating merkle tree: %v", err)
	}

	// Append operations with different hashes to test tree structure
	operations := []OpLogMerkleEntry{}
	for i := range 3 {
		ome := OpLogMerkleEntry{}
		ome.opLog = th.ops[i]
		ome.hash = ome.opLog.CanonicalKey()
		operations = append(operations, ome)

		err = m.AppendOp(ome)
		if err != nil {
			t.Fatalf("error appending operation %d: %v", i, err)
		}
	}

	// Verify all operations are in tree
	for i, ome := range operations {
		if !m.ContainsOp(ome) {
			t.Fatalf("operation %d should be in tree", i)
		}
	}

	snapshot1 := m.Snapshot()
	p := merkle.Prefix{
		Depth: 2,
		Path:  []uint8{1, 8},
	}
	summaries, err := snapshot1.Children(p)
	visualize := merkle.NewVisualizer(m)
	t.Logf("Tree: %s\n", visualize.VisualizeTree())

	if err != nil {
		t.Fatalf("error computing children: %v", err)
	}

	if len(summaries) != 16 {
		t.Errorf("expected 16 children, got %d", len(summaries))
	}

	for i := range summaries {
		t.Logf("Hash: %x, Count: %d, LastK: %x\n", summaries[i].Hash, summaries[i].Count, summaries[i].LastK)
	}
}

func Test_MERKLE_DiffDescent_09(t *testing.T) {
	cfg := merkle.Config{Fanout: 16, MaxDepth: 2, Hasher: NewTestHasher()}

	mA, _ := merkle.New(cfg)
	mB, _ := merkle.New(cfg)

	th := NewTestHasher()

	// Append the same 3 ops to both nodes
	for i := range 3 {
		ome := OpLogMerkleEntry{opLog: th.ops[i], hash: th.ops[i].CanonicalKey()}
		_ = mA.AppendOp(ome)
		_ = mB.AppendOp(ome)
	}

	// Take snapshots (both at the same root now)
	sA := mA.Snapshot()

	// Diverge B with one extra op, then snapshot
	extra := engine.GenerateOp()
	extraLog, _ := th.opLog.Append(&extra)
	e := OpLogMerkleEntry{opLog: *extraLog, hash: extraLog.CanonicalKey()}
	_ = mB.AppendOp(e)
	sB := mB.Snapshot()

	diffs, met, err := merkle.DiffDescent(
		sA,
		sB.Root(),
		sB.MaxDepth(),
		func(p merkle.Prefix) ([]merkle.Summary, error) { return sB.Children(p) }, // mock transport
	)
	if err != nil {
		t.Fatalf("diff failed: %v", err)
	}

	if len(diffs) != 1 {
		t.Fatalf("want 1 diff leaf-parent, got %d", len(diffs))
	}
	// Optional: check the exact path if you expect it
	t.Log(met.String())
}
