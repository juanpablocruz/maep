package merkle

import (
	"crypto/sha256"
	"testing"
)

func h(s string) [32]byte {
	var out [32]byte
	sum := sha256.Sum256([]byte(s))
	copy(out[:], sum[:])
	return out
}

func TestOrderIndependence(t *testing.T) {
	// Same leaves, different order -> same root
	a := []Leaf{{Key: "A", Hash: h("A1")}, {Key: "B", Hash: h("B1")}}
	b := []Leaf{{Key: "B", Hash: h("B1")}, {Key: "A", Hash: h("A1")}}
	r1 := Build(a)
	r2 := Build(b)
	if r1 != r2 {
		t.Fatalf("roots differ for same leaves in different order")
	}
}

func TestRootChangesOnLeafChange(t *testing.T) {
	l1 := []Leaf{{Key: "A", Hash: h("A1")}, {Key: "B", Hash: h("B1")}}
	l2 := []Leaf{{Key: "A", Hash: h("A2")}, {Key: "B", Hash: h("B1")}}
	if Build(l1) == Build(l2) {
		t.Fatalf("root did not change when a leaf changed")
	}
}
