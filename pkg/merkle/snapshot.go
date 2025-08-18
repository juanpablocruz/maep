package merkle

import (
	"bytes"
	"errors"
	"fmt"
	"time"

	"github.com/juanpablocruz/maep/pkg/metrics"
)

var (
	ErrStaleSnapshot = errors.New("snapshot is stale")
)

type MerkleSnapshot struct {
	fanout   int
	maxDepth int
	root     Hash
	epoch    uint64
	source   *Merkle
	hasher   Hasher
	metrics  *metrics.DescMetrics
}

func (s *MerkleSnapshot) Fanout() int   { return s.fanout }
func (s *MerkleSnapshot) MaxDepth() int { return s.maxDepth }
func (s *MerkleSnapshot) Root() Hash    { return s.root }
func (s *MerkleSnapshot) Count() int    { return int(s.source.root.Count) }

func (s *MerkleSnapshot) Children(p Prefix) ([]Summary, error) {
	if int(p.Depth) != len(p.Path) {
		return nil, fmt.Errorf("invalid prefix depth")
	}

	if int(p.Depth) > s.maxDepth {
		return nil, fmt.Errorf("prefix depth exceeds max depth")
	}

	m := s.source
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.root.Hash != s.root {
		return nil, ErrStaleSnapshot
	}

	node := m.root

	// walk from root to the node identified by the prefix
	for _, d := range p.Path[:p.Depth] {
		i := int(d)
		if i < 0 || i >= 16 {
			return nil, fmt.Errorf("invalid prefix digit")
		}
		if node.Child[i] == nil {
			out := make([]Summary, 16)
			return out, nil
		}
		node = node.Child[i]
	}

	out := make([]Summary, 16)
	for i := range 16 {
		if c := node.Child[i]; c != nil {
			out[i] = Summary{
				Hash:  c.Hash,
				Count: c.Count,
				LastK: c.LastK,
			}
		}
	}
	return out, nil
}

func (s *MerkleSnapshot) ProofForKey(key OpHash) (Proof, error) {
	m := s.source
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.root.Hash != s.root {
		return Proof{}, ErrStaleSnapshot
	}

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

	// Leaf set (sorted using the same hasher as tree construction)
	leafOps := make([]OpHash, 0, len(node.Ops))
	for h := range node.Ops {
		leafOps = append(leafOps, h)
	}
	s.hasher.Sort(leafOps)

	// Prepare proof: Nodes[0]=leaf set; Nodes[l]=15 siblings at ancestor l
	nodes := make([][]Hash, 0, len(idxs))
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

	return Proof{Fanout: 16, Leaf: leafOps, Nodes: nodes}, nil
}
func (s *MerkleSnapshot) Epoch() uint64 { return s.epoch }
func (s *MerkleSnapshot) Close()        {}

func DiffDescent(
	local Snapshot,
	remoteRoot Hash,
	remoteMaxDepth int,
	fetcher ChildrenFetcher,
) ([][]uint8, metrics.DescMetrics, error) {

	var met metrics.DescMetrics

	met.Fanout = local.Fanout()
	met.MaxDepth = local.MaxDepth()

	if remoteMaxDepth != met.MaxDepth {
		return nil, met, fmt.Errorf("remote max depth %d != local max depth %d", remoteMaxDepth, met.MaxDepth)
	}

	if local.Root() == remoteRoot {
		return nil, met, nil
	}

	type frame struct {
		pref  []uint8
		depth int
	}

	stack := []frame{{nil, 0}}
	depthSum := 0

	start := time.Now()

	var diffs [][]uint8

	for len(stack) > 0 {
		f := stack[len(stack)-1]
		stack = stack[:len(stack)-1]

		pref := Prefix{
			Depth: uint8(len(f.pref)),
			Path:  append([]uint8(nil), f.pref...),
		}

		ca, errA := local.Children(pref)
		if errA == ErrStaleSnapshot {
			met.Restarts++
			return nil, met, fmt.Errorf("snapshot restarted")
		}

		if errA != nil {
			return nil, met, errA
		}

		cb, errB := fetcher(pref)
		if errB != nil {
			return nil, met, fmt.Errorf("error fetching children: %v", errB)
		}

		met.NodesVisited++
		depthSum += f.depth
		met.HashComparisons += 16
		met.SummaryBytes += metrics.RespBytesPerNode

		equal := true
		for i := range 16 {
			if ca[i].Hash != cb[i].Hash {
				equal = false
				break
			}
		}

		if equal {
			continue
		}

		if f.depth == local.MaxDepth()-1 {
			met.LeavesTouched++
			diffs = append(diffs, append([]uint8(nil), f.pref...))

			// Δ := sum over differing children of |CountA - CountB|
			var deltaHere uint64
			for i := range 16 {
				if ca[i].Hash != cb[i].Hash {
					a, b := ca[i].Count, cb[i].Count
					if a >= b {
						deltaHere += a - b
					} else {
						deltaHere += b - a
					}
				}
			}
			met.Delta += int(deltaHere) // adjust type if your field is uint64
			continue
		}
		for i := range 16 {
			if ca[i].Hash != cb[i].Hash {
				child := append(append([]uint8{}, f.pref...), uint8(i))
				stack = append(stack, frame{child, f.depth + 1})
			}
		}
	}

	met.DurationMS = float64(time.Since(start).Microseconds()) / 1000.0
	if met.NodesVisited > 0 {
		met.AvgDepth = float64(depthSum) / float64(met.NodesVisited)
	}
	return diffs, met, nil

}

// VerifyOpProof checks membership of op under root using the given proof.
// Assumes proof.Leaf is the full leaf set (sorted),
// and proof.Nodes[t] are sibling lists for level t (15 per level).
func VerifyOpProof(root Hash, op OpHash, maxDepth int, proof Proof, hasher Hasher) bool {
	if proof.Fanout != 16 {
		return false
	}
	if len(proof.Nodes) != maxDepth {
		return false
	}

	for _, n := range proof.Nodes {
		if len(n) != 15 {
			return false
		}
	}

	L := append([]OpHash(nil), proof.Leaf...)
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
	h := hashLeafSet(L)

	digits, err := KeyDigits16(op, maxDepth)
	if err != nil {
		return false
	}

	for t := range maxDepth {
		idx := int(digits[maxDepth-1-t]) // bottom-most digit first
		sibs := proof.Nodes[t]           // 15 siblings for this level

		children := make([]Hash, 16)
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

		h = hashNode16([16]Hash(children)) // H(0x01 || h0 || ... || h15)
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
