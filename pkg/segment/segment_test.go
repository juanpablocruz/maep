package segment

import (
	"testing"

	"github.com/juanpablocruz/maep/pkg/materialize"
	"github.com/juanpablocruz/maep/pkg/merkle"
)

func TestForKey(t *testing.T) {
	tests := []struct {
		key  string
		want ID
	}{
		{"foo", 11302},
		{"bar", 64734},
		{"baz", 47781},
	}
	for _, tt := range tests {
		if got := ForKey(tt.key); got != tt.want {
			t.Fatalf("ForKey(%q) = %d, want %d", tt.key, got, tt.want)
		}
	}
}

func TestGroupLeaves(t *testing.T) {
	leaves := []merkle.Leaf{{Key: "foo"}, {Key: "bar"}, {Key: "foo"}}
	grouped := GroupLeaves(leaves)
	if len(grouped) != 2 {
		t.Fatalf("expected 2 segments, got %d", len(grouped))
	}
	if len(grouped[ForKey("foo")]) != 2 {
		t.Fatalf("foo segment should have 2 leaves")
	}
	if len(grouped[ForKey("bar")]) != 1 {
		t.Fatalf("bar segment should have 1 leaf")
	}
}

func TestRootsBySegment(t *testing.T) {
	view := map[string]materialize.State{
		"foo": {Present: true, Value: []byte("a")},
		"bar": {Present: true, Value: []byte("b")},
		"baz": {Present: true, Value: []byte("c")},
	}
	roots := RootsBySegment(view)

	leaves := materialize.LeavesFromSnapshot(view)
	bySeg := GroupLeaves(leaves)
	if len(roots) != len(bySeg) {
		t.Fatalf("got %d roots, want %d", len(roots), len(bySeg))
	}
	for sid, ls := range bySeg {
		want := merkle.Build(ls)
		if got := roots[sid]; got != want {
			t.Fatalf("root mismatch for segment %d", sid)
		}
	}
}
