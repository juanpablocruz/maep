package engine

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"iter"
	"sync"
)

// fixed-fanout k-ary Merkle

type MerkleHash [32]byte

type MerkleNodeType int

const (
	MerkleNodeTypeInner MerkleNodeType = iota
	MerkleNodeTypeLeaf
)

// Prefix is the path segment in the tree. It is the first d nybbles of the canonical key (HLC || ActorId || OpHash).
// This prefix states that the leaf covers everything starting with this hex path.
type Prefix struct {
	Depth uint8
	Path  []byte
}

type MerkleNode struct {
	Type MerkleNodeType
	// node at depth d has id first d nybbles of K
	ID     int
	Prefix Prefix
	// number of ops in this node
	Count int
	// max K in leaf
	LastK OpCannonicalKey

	// H(type || prefix || count || concat(opHash for ops in K order))
	Hash MerkleHash

	// k children
	Children []*MerkleNode

	parent *MerkleNode
}

type Merkle struct {
	Depth  uint8
	Fanout uint32
	root   *MerkleNode
	mu     sync.RWMutex
	getOps func(low, high OpCannonicalKey) iter.Seq2[OpCannonicalKey, *OpLogEntry]
}

func NewMerkle(fanout uint32, depth uint8, getOps func(low, high OpCannonicalKey) iter.Seq2[OpCannonicalKey, *OpLogEntry]) *Merkle {
	m := &Merkle{Depth: depth, Fanout: fanout, getOps: getOps}
	m.root = NewMerkleNode(false, nil, fanout)
	return m
}

var zeroHash = [32]byte{}

func (m *Merkle) Root() MerkleHash {
	if m.root == nil {
		return zeroHash
	}
	return m.root.Hash
}

// get the i-th 4-bit nybble of the key
func nyb(k []byte, i uint8, fanout uint32) int {
	bpl := bitsPerLevel(fanout)
	_, byteIdx, off := bitPosition(uint(i), bpl)

	if int(byteIdx) >= len(k) {
		return 0
	}

	w := createBitWindow(k, byteIdx)
	mask, shift := calculateMaskAndShift(off, bpl)
	return int((w >> shift) & mask)
}

func (m *Merkle) GetLeaf(K []byte) *MerkleNode {
	n := m.root

	for lvl := uint8(0); lvl < m.Depth; lvl++ {
		index := nyb(K, lvl, m.Fanout)

		if n.Children[index] == nil {
			n.Children[index] = NewMerkleNode(lvl+1 == m.Depth, n, m.Fanout)
		}
		n = n.Children[index]
	}
	return n
}

func (m *Merkle) NodeAt(p Prefix) *MerkleNode {
	if m.root == nil || p.Depth > m.Depth {
		return nil
	}

	// If depth is 0, return root
	if p.Depth == 0 {
		return m.root
	}

	n := m.root
	for lvl := uint8(0); lvl < p.Depth; lvl++ {
		idx := nyb(p.Path, lvl, m.Fanout)
		if n.Children == nil || n.Children[idx] == nil {
			return nil
		}
		n = n.Children[idx]
	}
	return n
}

func (m *Merkle) Append(e OpLogEntry) {
	// take first d nybbles of K
	K := e.key[:]
	leaf := m.GetLeaf(K)

	leaf.Count++
	if bytes.Compare(leaf.LastK[:], e.key[:]) < 0 {
		leaf.LastK = e.key
	}

	// Recompute hash: we need to fetch all the OpLogEntry that are in this leaf, add the new
	// one, sort them, and then recompute the hash.

	allOps := []*OpLogEntry{}

	for _, op := range m.getOps(leaf.LastK, e.key) {
		allOps = append(allOps, op)
	}

	h := sha256.New()
	for _, op := range allOps {
		h.Write(op.hash[:])
	}
	copy(leaf.Hash[:], h.Sum(nil))

	// bubble up hash
	for p := leaf.parent; p != nil; p = p.parent {
		h := sha256.New()
		h.Write([]byte{byte(MerkleNodeTypeInner)})
		for i := range m.Fanout {
			if c := p.Children[i]; c != nil {
				h.Write(c.Hash[:])
			} else {
				h.Write(zeroHash[:])
			}
		}
		copy(p.Hash[:], h.Sum(nil))
	}
}

func NewMerkleNode(isLeaf bool, parent *MerkleNode, fanout uint32) *MerkleNode {
	n := &MerkleNode{Type: MerkleNodeTypeInner, parent: parent}
	if isLeaf {
		n.Type = MerkleNodeTypeLeaf
	} else {
		n.Children = make([]*MerkleNode, int(fanout))
	}
	return n
}

func (n *MerkleNode) GetHash() MerkleHash {
	var buf bytes.Buffer

	binary.Write(&buf, binary.BigEndian, int32(n.Type))
	binary.Write(&buf, binary.BigEndian, int32(n.Prefix.Depth))
	buf.Write(n.Prefix.Path)
	binary.Write(&buf, binary.BigEndian, int32(n.Count))
	binary.Write(&buf, binary.BigEndian, n.LastK[:])

	h := sha256.New()
	h.Write(buf.Bytes())
	var hash MerkleHash
	copy(hash[:], h.Sum(nil))
	return hash
}

// Summarize Returns k child hashes at prefix p
// ok=false if p.depth > m.depth
func (m *Merkle) Summarize(p Prefix) (children []MerkleHash, ok bool) {
	if p.Depth > m.Depth {
		return nil, false
	}
	hashes := make([]MerkleHash, m.Fanout)
	n := m.NodeAt(p)
	if n == nil {
		for i := range hashes {
			hashes[i] = zeroHash
		}
		return hashes, true
	}

	for i := range m.Fanout {
		if n.Children != nil && n.Children[i] != nil {
			hashes[i] = n.Children[i].GetHash()
		} else {
			hashes[i] = zeroHash
		}
	}
	return hashes, true
}

// collectLeaves iteratively collects all populated leaves under the given prefix
func (m *Merkle) collectLeaves(prefix Prefix) []*MerkleNode {
	if m.root == nil {
		return []*MerkleNode{}
	}
	leaves := make([]*MerkleNode, 0, m.Fanout)

	// Navigate to the node at the given prefix first
	current := m.NodeAt(prefix)
	// Now collect all leaves under this node
	stack := []*MerkleNode{current}

	for len(stack) > 0 {
		current := stack[len(stack)-1]
		stack = stack[:len(stack)-1]

		if current.Type == MerkleNodeTypeLeaf && current.Count > 0 {
			leaves = append(leaves, current)
		} else {
			// Push children onto stack in reverse order to maintain traversal order
			for i := len(current.Children) - 1; i >= 0; i-- {
				if current.Children[i] != nil {
					stack = append(stack, current.Children[i])
				}
			}
		}
	}
	return leaves
}

func (m *Merkle) Leaves(p Prefix) iter.Seq2[MerkleHash, *MerkleNode] {
	return func(yield func(MerkleHash, *MerkleNode) bool) {
		leaves := m.collectLeaves(p)
		for _, leaf := range leaves {
			if !yield(leaf.Hash, leaf) {
				return
			}
		}
	}
}
