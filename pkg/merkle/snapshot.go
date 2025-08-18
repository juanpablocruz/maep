package merkle

import (
	"bytes"
	"fmt"
	"slices"
)

type MerkleSnapshot struct {
	fanout   int
	maxDepth int
	root     Hash
	epoch    uint64
	source   *Merkle
	hasher   Hasher
}

func (m *MerkleSnapshot) Fanout() int   { return m.fanout }
func (m *MerkleSnapshot) MaxDepth() int { return m.maxDepth }
func (m *MerkleSnapshot) Root() Hash    { return m.root }
func (m *MerkleSnapshot) Children(p Prefix) ([]Summary, error) {
	return nil, nil
}
func (s *MerkleSnapshot) ProofForKey(key Hash) (Proof, error) {
	m := s.source
	m.mu.RLock()
	defer m.mu.RUnlock()
	digits, err := KeyDigits16(key, s.maxDepth)
	if err != nil {
		return Proof{}, err
	}

	node := m.root
	path := make([]*MerkleNode, 0, m.maxDepth+1)
	path = append(path, node)
	idxs := make([]int, 0, m.maxDepth)

	for _, d := range digits {
		i := int(d)
		if node.Child[i] == nil {
			return Proof{}, fmt.Errorf("op not present (missing child)")
		}
		node = node.Child[i]
		path = append(path, node)
		idxs = append(idxs, i)
	}

	if node.Ops == nil {
		return Proof{}, fmt.Errorf("op not present (empty bucket)")
	}
	if _, ok := node.Ops[key]; !ok {
		return Proof{}, fmt.Errorf("op not present (not in bucket)")
	}

	// Prepare proof: Nodes[0]=leaf set; Nodes[l]=15 siblings at ancestor l
	nodes := make([][]Hash, 0, len(idxs)+1)

	// Leaf set (sorted using the same hasher as tree construction)
	leafOps := make([]Hash, 0, len(node.Ops))
	for h := range node.Ops {
		leafOps = append(leafOps, h)
	}
	s.hasher.Sort(leafOps)
	nodes = append(nodes, leafOps)

	// Ancestors: collect siblings (15 hashes per level)
	// path = [root, ..., leaf]; idxs = child index at each level
	for level := len(idxs) - 1; level >= 0; level-- {
		parent := path[level]
		childIdx := idxs[level]
		sibs := make([]Hash, 0, 15)
		for i := range 16 {
			if i == childIdx {
				continue
			}
			if c := parent.Child[i]; c != nil {
				sibs = append(sibs, c.Hash)
			} else {
				sibs = append(sibs, zeroHash())
			}
		}
		nodes = append(nodes, sibs)
	}

	return Proof{Fanout: 16, Nodes: nodes}, nil
}
func (m *MerkleSnapshot) Epoch() uint64 { return m.epoch }
func (m *MerkleSnapshot) Close()        {}

// VerifyOpProof checks membership of op under root using the given proof.
// Assumes proof.Nodes[0] is the full leaf set (sorted),
// and proof.Nodes[1..] are sibling lists from leaf->root (15 per level).
func VerifyOpProof(root Hash, op Hash, maxDepth int, proof Proof) bool {
	if proof.Fanout != 16 {
		return false
	}
	if len(proof.Nodes) == 0 {
		return false
	}

	// 1) recompute leaf hash from full set; check op is present
	leafSet := slices.Clone(proof.Nodes[0]) // copy - already sorted by hasher
	// op must be in the set
	found := false
	for _, h := range leafSet {
		if bytes.Equal(h[:], op[:]) {
			found = true
			break
		}
	}
	if !found {
		return false
	}
	h := hashLeafSet(leafSet)

	// 2) climb to root using siblings
	digits, err := KeyDigits16(op, maxDepth)
	if err != nil {
		return false
	}
	if len(proof.Nodes) != 1+len(digits) {
		return false
	} // leaf + one row per level

	// From leaf upward: at each level, insert h at index 'idx' among 16 children and hash
	for levelFromLeaf := range digits {
		idx := int(digits[len(digits)-1-levelFromLeaf]) // bottom-up
		sibs := proof.Nodes[1+levelFromLeaf]            // 15 hashes

		// Rebuild the 16 child array in order 0..15, putting h at idx
		var children [16]Hash
		si := 0
		for i := range 16 {
			if i == idx {
				children[i] = h
			} else {
				children[i] = sibs[si]
				si++
			}
		}
		h = hashNode16(children)
	}
	return h == root
}
