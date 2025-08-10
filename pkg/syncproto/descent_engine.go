package syncproto

import (
	"slices"

	"github.com/juanpablocruz/maep/pkg/materialize"
	"github.com/juanpablocruz/maep/pkg/merkle"
)

// leavesWithPrefix returns the subset of leaves whose Key has the given byte prefix.
func leavesWithPrefix(all []merkle.Leaf, p []byte) []merkle.Leaf {
	out := make([]merkle.Leaf, 0, 64)
	for _, lf := range all {
		if hasPrefix(lf.Key, p) {
			out = append(out, lf)
		}
	}
	return out
}

func hasPrefix(s string, p []byte) bool {
	if len(p) > len(s) {
		return false
	}
	for i := range p {
		if s[i] != p[i] {
			return false
		}
	}
	return true
}

// groupChildren groups a span by the next label after the prefix.
func groupChildren(span []merkle.Leaf, p []byte) map[byte][]merkle.Leaf {
	m := make(map[byte][]merkle.Leaf)
	plen := len(p)
	for _, lf := range span {
		if len(lf.Key) <= plen {
			continue
		}
		lb := lf.Key[plen]
		m[lb] = append(m[lb], lf)
	}
	return m
}

// BuildDescentResp constructs a DescentResp for a prefix using the provided view and K threshold.
func BuildDescentResp(view map[string]materialize.State, prefix []byte, leafK int) DescentResp {
	// Build sorted leaves once for this view
	ls := materialize.LeavesFromSnapshot(view)
	slices.SortFunc(ls, func(a, b merkle.Leaf) int {
		if a.Key < b.Key {
			return -1
		}
		if a.Key > b.Key {
			return 1
		}
		return 0
	})
	span := leavesWithPrefix(ls, prefix)
	if len(span) <= leafK {
		out := make([]LeafHash, 0, len(span))
		for _, lf := range span {
			out = append(out, LeafHash{Key: lf.Key, Hash: lf.Hash})
		}
		return DescentResp{Prefix: slices.Clone(prefix), Leaves: out}
	}
	groups := groupChildren(span, prefix)
	kids := make([]ChildHash, 0, len(groups))
	for lb, g := range groups {
		h := merkle.Build(g)
		kids = append(kids, ChildHash{Label: lb, Hash: h, Count: uint32(len(g))})
	}
	slices.SortFunc(kids, func(a, b ChildHash) int {
		if a.Label < b.Label {
			return -1
		}
		if a.Label > b.Label {
			return 1
		}
		return 0
	})
	return DescentResp{Prefix: slices.Clone(prefix), Children: kids}
}

// DiffDescent compares a descent response versus local leaves and returns precise Needs
// for leaf mismatches and the next child prefixes that differ.
func DiffDescent(localAll []merkle.Leaf, resp DescentResp, counts map[string]int) (needs []Need, nextPrefixes [][]byte) {
	if len(resp.Children) > 0 {
		for _, ch := range resp.Children {
			p := append(slices.Clone(resp.Prefix), ch.Label)
			span := leavesWithPrefix(localAll, p)
			lh := merkle.Build(span)
			if lh != ch.Hash {
				nextPrefixes = append(nextPrefixes, p)
			}
		}
		return needs, nextPrefixes
	}

	if len(resp.Leaves) > 0 {
		localMap := make(map[string][32]byte, len(resp.Leaves))
		for _, lf := range leavesWithPrefix(localAll, resp.Prefix) {
			localMap[lf.Key] = lf.Hash
		}
		for _, rlf := range resp.Leaves {
			if localMap[rlf.Key] != rlf.Hash {
				fromCnt := uint32(0)
				if c, ok := counts[rlf.Key]; ok && c > 0 {
					if c > int(^uint32(0)) {
						fromCnt = ^uint32(0)
					} else {
						fromCnt = uint32(c)
					}
				}
				needs = append(needs, Need{Key: rlf.Key, From: fromCnt})
			}
		}
	}
	return needs, nil
}
