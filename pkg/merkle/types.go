package merkle

type Hash [32]byte

// Prefix identifies a subtree by base-k digits of a canonical key hash.
// Path[i] ∈ [0..k-1], len(Path) == Depth.
type Prefix struct {
	Depth uint8
	Path  []uint8
}

type Summary struct {
	Hash  Hash   // Subtree digest
	Count uint64 // total ops under this child subtree
	LastK Hash   // MAEP ordered max key hash present in this chils, zero if empty
}

type Proof struct {
	Fanout int
	Nodes  [][]Hash
}

type MerkleEntry interface {
	ComputeHash() Hash
}

type Hasher interface {
	Sort([]Hash)
}

// Snapshot is an immutable point-in-time view for the summary path.
type Snapshot interface {
	// Shape
	Fanout() int
	MaxDepth() int

	// Root digets for the whole tree.
	Root() Hash

	// Children returns exactly K Summary items for the given prefix.
	// for out-of-range prefixes, returns an error
	Children(p Prefix) ([]Summary, error)

	ProofForKey(key Hash) (Proof, error)
	Epoch() uint64 // passive epoch tag for observability

	// Release resources associated with the snapshot
	Close()
}

// Tree is the concurrency-safe, mutable structure
// All read paths for sync must use a Snapshot captured at session open
type Tree interface {
	Fanout() int
	MaxDepth() int

	// Create a point-in-time view.
	Snapshot() Snapshot

	AppendOp(MerkleEntry) error
	ContainsOp(e MerkleEntry) bool

	Stats() Stats

	Close() error
}

// stats for metric/observability
type Stats struct {
	Keys        uint64 // distinct non-zero-weight keys
	TotalOps    uint64 // sum of weights
	MaxDepth    int
	Fanout      int
	Epoch       uint64
	BytesOnHeap uint64
}

// Update is a single key weight change
type Update struct {
	Key   Hash
	Delta int64
}
