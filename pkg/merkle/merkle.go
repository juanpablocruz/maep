package merkle

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"sort"
	"sync"
	"time"
)

type Config struct {
	Fanout   int
	MaxDepth int
	Hasher   Hasher
}

type Merkle struct {
	fanout   int
	maxDepth int

	root   *MerkleNode
	hasher Hasher
	mu     sync.RWMutex
}

type MerkleNode struct {
	Hash  Hash
	Count uint64
	LastK Hash
	Child [16]*MerkleNode

	Ops map[Hash]struct{}
}

func zeroHash() Hash { return Hash{} }

func New(cfg Config) (*Merkle, error) {
	// Validate configuration
	if cfg.MaxDepth < 0 {
		return nil, fmt.Errorf("max depth cannot be negative: %d", cfg.MaxDepth)
	}
	if cfg.Fanout < 0 {
		return nil, fmt.Errorf("fanout cannot be negative: %d", cfg.Fanout)
	}
	if cfg.Hasher == nil {
		return nil, fmt.Errorf("hasher cannot be nil")
	}

	defaultFanout := 16
	if cfg.Fanout != 0 {
		defaultFanout = cfg.Fanout
	}
	return &Merkle{
		fanout:   defaultFanout,
		maxDepth: cfg.MaxDepth,
		root:     &MerkleNode{},
		hasher:   cfg.Hasher,
	}, nil
}

func (m *Merkle) Fanout() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.fanout
}

func (m *Merkle) MaxDepth() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.maxDepth
}

func (m *Merkle) Snapshot() Snapshot {
	return &MerkleSnapshot{
		fanout:   m.fanout,
		maxDepth: m.maxDepth,
		root:     m.root.Hash,
		epoch:    uint64(time.Now().UnixNano()),
	}
}

func (m *Merkle) Stats() *Stats {
	return &Stats{}
}

func (m *Merkle) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return nil
}

// ContainsOp checks if an operation exists in the tree
func (m *Merkle) ContainsOp(e MerkleEntry) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	opHash := e.ComputeHash()
	digits, err := KeyDigits16(opHash, m.maxDepth)
	if err != nil {
		return false
	}

	node := m.root
	for _, idx := range digits {
		i := int(idx)
		if node.Child[i] == nil {
			return false
		}
		node = node.Child[i]
	}

	if node.Ops == nil {
		return false
	}
	_, exists := node.Ops[opHash]
	return exists
}

func (m *Merkle) AppendOp(e MerkleEntry) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Check for nil entry
	if e == nil {
		return fmt.Errorf("cannot append nil entry")
	}

	// compute radix, this is take the key and split it into digits in base k (fanout)
	opHash := e.ComputeHash()
	digits, err := KeyDigits16(opHash, m.maxDepth)
	if err != nil {
		return err
	}

	path := make([]*MerkleNode, 0, m.maxDepth+1)
	node := m.root
	path = append(path, node)

	for _, idx := range digits {
		i := int(idx)
		if node.Child[i] == nil {
			node.Child[i] = &MerkleNode{}
		}
		node = node.Child[i]
		path = append(path, node)
	}

	leaf := node
	if leaf.Ops == nil {
		leaf.Ops = make(map[Hash]struct{})
	}
	if _, exists := leaf.Ops[opHash]; exists {
		// idempotent
		return nil
	}

	leaf.Ops[opHash] = struct{}{}

	ops := make([]Hash, 0, len(leaf.Ops))
	var maxK Hash
	for h := range leaf.Ops {
		ops = append(ops, h)
	}
	m.hasher.Sort(ops)
	maxK = ops[len(ops)-1]

	leaf.Hash = hashLeafSet(ops)
	leaf.Count = uint64(len(ops))
	leaf.LastK = maxK

	for up := len(path) - 2; up >= 0; up-- {
		parent := path[up]

		var children [16]Hash
		var sum uint64
		var maxH Hash

		for i := range 16 {
			if c := parent.Child[i]; c != nil {
				children[i] = c.Hash
				sum += c.Count
				ops := []Hash{c.LastK, maxH}
				m.hasher.Sort(ops)
				maxH = ops[1]
			} else {
				children[i] = zeroHash()
			}
		}
		parent.Hash = hashNode16(children)
		parent.Count = sum
		parent.LastK = maxH
	}
	return nil
}

func hashNode16(children [16]Hash) Hash {
	h := sha256.New()
	h.Write([]byte{0x01})
	for i := range 16 {
		h.Write(children[i][:])
	}
	var out Hash
	copy(out[:], h.Sum(nil))
	return out
}

func hashLeafSet(ops []Hash) Hash {
	if len(ops) == 0 {
		return zeroHash()
	}

	sort.Slice(ops, func(i, j int) bool {
		return bytes.Compare(ops[i][:], ops[j][:]) < 0
	})

	h := sha256.New()
	h.Write([]byte{0x04})
	for _, x := range ops {
		h.Write(x[:])
	}
	var out Hash
	copy(out[:], h.Sum(nil))
	return out
}
