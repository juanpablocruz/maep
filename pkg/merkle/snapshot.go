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
	ErrOpNotPresent  = errors.New("op not present")
	ErrEmptyBucket   = errors.New("empty bucket")
	ErrOpNotInBucket = errors.New("op not in bucket")
	ErrInvalidPrefix = errors.New("invalid prefix")
	ErrInvalidDigit  = errors.New("invalid prefix digit")
	ErrPrefixDepth   = errors.New("prefix depth exceeds max depth")
	ErrChildNotFound = errors.New("child not found")
	ErrLeafNotFound  = errors.New("leaf not found")
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
		return nil, ErrInvalidPrefix
	}

	if int(p.Depth) > s.maxDepth {
		return nil, ErrPrefixDepth
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
			return nil, ErrInvalidDigit
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
			return Proof{}, ErrOpNotPresent
		}
		node = node.Child[i]
		path = append(path, node)
		idxs = append(idxs, i)
	}

	if node.Ops == nil {
		return Proof{}, ErrEmptyBucket
	}
	if _, ok := node.Ops[key]; !ok {
		return Proof{}, ErrOpNotInBucket
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

func sumCounts(children []Summary) uint64 {
	var s uint64
	for i := range 16 {
		s += children[i].Count
	}
	return s
}

type MetricLogger func(any)

func DiffDescent(
	local Snapshot,
	remoteRoot Hash,
	remoteMaxDepth int,
	fetcher ChildrenFetcher,
	instrument MetricLogger,
) ([][]uint8, error) {

	met := metrics.DescMetrics{
		Fanout:   local.Fanout(),
		MaxDepth: local.MaxDepth(),
	}
	instrument(met)

	root := Prefix{Depth: 0, Path: nil}
	caRoot, err := local.Children(root)
	if err == ErrStaleSnapshot {
		met.Restarts++
		instrument(met)
		return nil, ErrStaleSnapshot
	}
	if err != nil {
		return nil, err
	}
	met.M = int(sumCounts(caRoot))
	instrument(met)

	if remoteMaxDepth != met.MaxDepth {
		return nil, fmt.Errorf("remote max depth %d != local max depth %d", remoteMaxDepth, met.MaxDepth)
	}

	if local.Root() == remoteRoot {
		return nil, nil
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
			instrument(met)
			return nil, ErrStaleSnapshot
		}

		if errA != nil {
			return nil, errA
		}

		cb, errB := fetcher(pref)
		if errB != nil {
			return nil, fmt.Errorf("error fetching children: %v", errB)
		}

		met.NodesVisited++
		depthSum += f.depth
		met.HashComparisons += 16
		met.SummaryBytes += metrics.RespBytesPerNode
		instrument(met)

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
			instrument(met)
			diffs = append(diffs, append([]uint8(nil), f.pref...))

			// delta := sum over differing children of |CountA - CountB|
			var delta uint64
			for i := range 16 {
				if ca[i].Hash != cb[i].Hash {
					a, b := ca[i].Count, cb[i].Count
					if a >= b {
						delta += a - b
					} else {
						delta += b - a
					}
				}
			}
			met.Delta += int(delta) // adjust type if your field is uint64
			instrument(met)
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
	instrument(met)
	return diffs, nil

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

func (s *MerkleSnapshot) LeafOps(parent Prefix, child uint8) ([]OpHash, error) {
	m := s.source
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.root.Hash != s.root {
		return nil, ErrStaleSnapshot
	}

	node := m.root
	for _, d := range parent.Path {
		if node.Child[int(d)] == nil {
			return nil, ErrChildNotFound
		}
		node = node.Child[int(d)]
	}

	leaf := node.Child[int(child)]
	if leaf == nil || leaf.Ops == nil {
		return nil, ErrLeafNotFound
	}

	out := make([]OpHash, 0, len(leaf.Ops))
	for h := range leaf.Ops {
		out = append(out, h)
	}
	s.hasher.Sort(out)
	return out, nil
}

// DeltaBytes runs summary descent, then fetches leaf keys for differing children,
// computes exact delta (remote -> local), and totals wire bytes.
func DeltaBytes(
	local Snapshot,
	remoteRoot Hash,
	remoteMaxDepth int,
	fetchSummary ChildrenFetcher, // remote Children()
	fetchLeaf LeafKeysFetcher, // remote LeafOps()
	avgOpBytes int,
	logger MetricLogger,
) error {

	metricsLogger := metrics.NewMetricsLogger()
	metricsLogger.Start()
	defer metricsLogger.Close()

	// summary descent
	diffs, err := DiffDescent(local, remoteRoot, remoteMaxDepth, fetchSummary, metricsLogger.Log)
	if err != nil {
		return err
	}

	sumMet := metricsLogger.ReadDesc()

	tm := metrics.TransferMetrics{
		M:             sumMet.M,
		LeavesTouched: sumMet.LeavesTouched,
		SummaryBytes:  sumMet.SummaryBytes,
	}

	// For each differing leaf parent, fetch both sides' children to know which child indexes differ
	for _, pref := range diffs {
		p := Prefix{Depth: uint8(len(pref)), Path: pref}
		ca, errA := local.Children(p)
		if errA != nil {
			logger(tm)
			return errA
		}
		cb, errB := fetchSummary(p)
		if errB != nil {
			logger(tm)
			return errB
		}

		for i := range 16 {
			if ca[i].Hash == cb[i].Hash {
				continue
			}

			// Fetch leaf key lists (remote then local)
			remoteKeys, err := fetchLeaf(p, uint8(i))
			if err != nil {
				logger(tm)
				return err
			}
			localKeys, err := local.LeafOps(p, uint8(i))
			if err != nil {
				// If local leaf doesn't exist, treat as empty list
				if err == ErrChildNotFound || err == ErrLeafNotFound {
					localKeys = []OpHash{}
				} else {
					logger(tm)
					return err
				}
			}

			// Account bytes for the remote reply with its key list
			tm.LeafKeysBytes += int64(len(remoteKeys)) * 64

			// Exact delta (one-way: B\A) via two-pointer diff
			j, k := 0, 0
			for j < len(remoteKeys) && k < len(localKeys) {
				cmp := bytes.Compare(remoteKeys[j][:], localKeys[k][:])
				switch {
				case cmp == 0:
					j++
					k++
				case cmp < 0:
					tm.DeltaExact++
					tm.OpsBytes += int64(avgOpBytes)
					j++
				default:
					k++
				}
			}
			// tail in remote
			if j < len(remoteKeys) {
				remaining := len(remoteKeys) - j
				tm.DeltaExact += remaining
				tm.OpsBytes += int64(remaining * avgOpBytes)
			}
		}
	}

	tm.TotalBytes = tm.SummaryBytes + tm.LeafKeysBytes + tm.OpsBytes
	logger(tm)
	return nil
}
