package syncproto

import (
	"testing"

	"github.com/juanpablocruz/maep/pkg/materialize"
	"github.com/juanpablocruz/maep/pkg/merkle"
)

// helper to build a materialize view map for keys with values
func buildView(pairs map[string][]byte) map[string]materialize.State {
	view := make(map[string]materialize.State, len(pairs))
	for k, v := range pairs {
		cp := make([]byte, len(v))
		copy(cp, v)
		view[k] = materialize.State{Present: true, Value: cp}
	}
	return view
}

// Spec: BuildDescentResp returns Leaves when span <= K, otherwise Children grouped by next label
func TestBuildDescentResp_LeafVsChildrenThreshold(t *testing.T) {
	view := buildView(map[string][]byte{
		"a1": []byte("x"),
		"a2": []byte("y"),
		"b1": []byte("p"),
		"b2": []byte("q"),
		"b3": []byte("r"),
	})
	// K small → expect Children at root
	resp := BuildDescentResp(view, nil, 3)
	if len(resp.Children) == 0 || len(resp.Leaves) != 0 {
		t.Fatalf("expected children at root span > K")
	}
	// Validate children grouping and counts
	all := materialize.LeavesFromSnapshot(view)
	groups := groupChildren(leavesWithPrefix(all, nil), nil)
	if len(groups) != len(resp.Children) {
		t.Fatalf("children count mismatch: got %d want %d", len(resp.Children), len(groups))
	}
	// Check each child hash matches merkle over its group
	for _, ch := range resp.Children {
		g := groups[ch.Label]
		want := merkle.Build(g)
		if want != ch.Hash {
			t.Fatalf("child hash mismatch for label %q", ch.Label)
		}
	}

	// K large → expect Leaves
	resp2 := BuildDescentResp(view, nil, 10)
	if len(resp2.Leaves) != len(all) || len(resp2.Children) != 0 {
		t.Fatalf("expected leaves when span <= K")
	}
}

// Spec: DiffDescent detects mismatched children (next prefixes) and leaf mismatches (needs)
func TestDiffDescent_DetectsMismatchedChildrenAndLeaves(t *testing.T) {
	// Local view
	view := buildView(map[string][]byte{
		"a1": []byte("x"),
		"a2": []byte("y"),
		"b1": []byte("p"),
		"b2": []byte("q"),
	})
	localAll := materialize.LeavesFromSnapshot(view)

	// Remote child response at root with one mismatched child 'a'
	groups := groupChildren(leavesWithPrefix(localAll, nil), nil)
	var children []ChildHash
	for lb, g := range groups {
		h := merkle.Build(g)
		if lb == 'a' {
			// flip hash to force mismatch
			var bogus [32]byte
			bogus[0] = 0xFF
			children = append(children, ChildHash{Label: lb, Hash: bogus, Count: uint32(len(g))})
		} else {
			children = append(children, ChildHash{Label: lb, Hash: h, Count: uint32(len(g))})
		}
	}
	needs, next := DiffDescent(localAll, DescentResp{Prefix: nil, Children: children}, map[string]int{"a1": 1, "a2": 2, "b1": 1, "b2": 2})
	if len(needs) != 0 || len(next) != 1 || len(next[0]) != 1 || next[0][0] != 'a' {
		t.Fatalf("expected nextPrefixes ['a'], got needs=%v next=%v", needs, next)
	}

	// Remote leaves response for prefix 'a' with one leaf mismatched
	spanA := leavesWithPrefix(localAll, []byte{'a'})
	// corrupt hash of a2 only
	leaves := make([]LeafHash, 0, len(spanA))
	for _, lf := range spanA {
		if lf.Key == "a2" {
			var bogus [32]byte
			bogus[0] = 0xEE
			leaves = append(leaves, LeafHash{Key: lf.Key, Hash: bogus})
		} else {
			leaves = append(leaves, LeafHash{Key: lf.Key, Hash: lf.Hash})
		}
	}
	needs2, next2 := DiffDescent(localAll, DescentResp{Prefix: []byte{'a'}, Leaves: leaves}, map[string]int{"a1": 1, "a2": 2, "b1": 1, "b2": 2})
	if len(next2) != 0 || len(needs2) != 1 || needs2[0].Key != "a2" || needs2[0].From != 2 {
		t.Fatalf("expected need for a2 with From=2, got needs=%v next=%v", needs2, next2)
	}
}
