package node

import (
	"testing"
	"time"

	"github.com/juanpablocruz/maep/pkg/hlc"
	"github.com/juanpablocruz/maep/pkg/materialize"
	"github.com/juanpablocruz/maep/pkg/merkle"
	"github.com/juanpablocruz/maep/pkg/model"
	"github.com/juanpablocruz/maep/pkg/transport"
)

// Spins up 3 nodes with in-memory transport and verifies they converge after quiescing writes.
func TestThreeNodeConvergesAfterQuiesce(t *testing.T) {
	sw := transport.NewSwitch()
	// Listen addresses
	ep0, _ := sw.Listen("N0")
	ep1, _ := sw.Listen("N1")
	ep2, _ := sw.Listen("N2")
	defer ep0.Close()
	defer ep1.Close()
	defer ep2.Close()

	// Ring peers
	n0 := New("N0", ep0, "N1", 200*time.Millisecond)
	n1 := New("N1", ep1, "N2", 200*time.Millisecond)
	n2 := New("N2", ep2, "N0", 200*time.Millisecond)
	n0.AttachEvents(make(chan Event, 1024))
	n1.AttachEvents(make(chan Event, 1024))
	n2.AttachEvents(make(chan Event, 1024))
	n0.Clock, n1.Clock, n2.Clock = hlc.New(), hlc.New(), hlc.New()
	n0.Start()
	defer n0.Stop()
	n1.Start()
	defer n1.Stop()
	n2.Start()
	defer n2.Stop()

	// Seed a few writes on N0 only
	var a0, a1, a2 model.ActorID
	a0[0], a1[0], a2[0] = 1, 2, 3
	n0.Put("A", []byte("v1"), a0)
	n0.Put("B", []byte("v1"), a0)
	n0.Delete("B", a0)
	n0.Put("C", []byte("v1"), a0)

	// Let syncs happen for a bit
	time.Sleep(2 * time.Second)

	// Quiesce: no new writes; allow propagation to settle
	// Wait up to a few seconds for roots to match
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		v0 := materialize.Snapshot(n0.Log)
		v1 := materialize.Snapshot(n1.Log)
		v2 := materialize.Snapshot(n2.Log)
		r0 := merkle.Build(materialize.LeavesFromSnapshot(v0))
		r1 := merkle.Build(materialize.LeavesFromSnapshot(v1))
		r2 := merkle.Build(materialize.LeavesFromSnapshot(v2))
		if r0 == r1 && r1 == r2 {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("roots did not converge within timeout")
}
