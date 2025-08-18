package merkle

import (
	"bytes"
	"fmt"
)

type MerkleSnapshot struct {
	fanout   int
	maxDepth int
	root     Hash
	epoch    uint64
	source   *Merkle
	hasher   Hasher
}

func (s *MerkleSnapshot) Fanout() int   { return s.fanout }
func (s *MerkleSnapshot) MaxDepth() int { return s.maxDepth }
func (s *MerkleSnapshot) Root() Hash    { return s.root }
func (s *MerkleSnapshot) Children(p Prefix) ([]Summary, error) {
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
func (s *MerkleSnapshot) Epoch() uint64 { return s.epoch }
func (s *MerkleSnapshot) Close()        {}

// VerifyOpProof checks membership of op under root using the given proof.
// Assumes proof.Nodes[0] is the full leaf set (sorted),
// and proof.Nodes[1..] are sibling lists from leaf->root (15 per level).
func VerifyOpProof(root Hash, op Hash, maxDepth int, proof Proof, hasher Hasher) bool {
	if proof.Fanout != 16 {
		return false
	}
	if len(proof.Nodes) != 1+maxDepth {
		return false
	}

	for i, n := range proof.Nodes {
		if i == 0 {
			continue
		}
		if len(n) != 15 {
			return false
		}
	}

	L := make([]Hash, len(proof.Nodes[0]))
	copy(L, proof.Nodes[0])

	hasher.Sort(L)

	found := false
	for _, l := range L {
		if bytes.Equal(l[:], op[:]) {
			found = true
			break
		}
	}
	if !found {
		return false
	}

	// Hash leaf without re-sorting
	h := hashLeafSet(proof.Nodes[0])

	digits, err := KeyDigits16(op, maxDepth)
	if err != nil {
		return false
	}

	for t := range maxDepth {
		idx := int(digits[maxDepth-1-t]) // bottom-most digit first
		sibs := proof.Nodes[1+t]         // 15 siblings for this level

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
		if si != 15 {
			return false
		} // sanity
		h = hashNode16(children) // H(0x01 || h0 || ... || h15)
	}
	return h == root
}

func (p Proof) String() string {
	s := fmt.Sprintf("Fanout: %d\n", p.Fanout)
	for i, n := range p.Nodes {
		for j, h := range n {
			s += fmt.Sprintf("Node %d-%d: %x\n", i, j, h)
		}
	}
	return s
}
