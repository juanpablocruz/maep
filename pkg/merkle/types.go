package merkle

type Hash [32]byte
type OpHash [64]byte

// Prefix identifies a subtree by base-k digits of a canonical key hash.
// Path[i] in [0..k-1], len(Path) == Depth.
type Prefix struct {
	Depth uint8
	Path  []uint8
}

type Summary struct {
	Hash  Hash   // Subtree digest
	Count uint64 // total ops under this child subtree
	LastK OpHash // MAEP ordered max key hash present in this chils, zero if empty
}

type Proof struct {
	Fanout int
	Leaf   []OpHash
	Nodes  [][]Hash
}

type MerkleEntry interface {
	ComputeHash() OpHash
}

type Hasher interface {
	Sort([]OpHash)
}

// Snapshot is an immutable point-in-time view for the summary path.
type Snapshot interface {
	// Shape
	Fanout() int
	MaxDepth() int
	Count() int
	// Root digets for the whole tree.
	Root() Hash

	// Children returns exactly K Summary items for the given prefix.
	// for out-of-range prefixes, returns an error
	Children(p Prefix) ([]Summary, error)

	ProofForKey(key OpHash) (Proof, error)
	Epoch() uint64 // passive epoch tag for observability

	LeafOps(parent Prefix, child uint8) ([]OpHash, error)
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
}

// Stats for metric/observability
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

type ChildrenFetcher func(p Prefix) ([]Summary, error)
type LeafKeysFetcher func(parent Prefix, child uint8) ([]OpHash, error)
