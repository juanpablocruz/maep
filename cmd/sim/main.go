package main

import (
	"bytes"
	"fmt"
	"time"

	"github.com/juanpablocruz/maep/pkg/materialize"
	"github.com/juanpablocruz/maep/pkg/model"
	"github.com/juanpablocruz/maep/pkg/node"
	"github.com/juanpablocruz/maep/pkg/transport"
)

// sim runs two in-memory nodes and prints their final state after a short
// exchange. It is useful for verifying that the replication algorithm
// converges even when wrapped in Chaos endpoints.
func main() {
	sw := transport.NewSwitch()
	epA, _ := sw.Listen("A")
	epB, _ := sw.Listen("B")
	epAHB, _ := sw.Listen("A_hb")
	epBHB, _ := sw.Listen("B_hb")

	chaosA := transport.WrapChaos(epA, transport.ChaosConfig{Up: true})
	chaosB := transport.WrapChaos(epB, transport.ChaosConfig{Up: true})
	chaosAHB := transport.WrapChaos(epAHB, transport.ChaosConfig{Up: true})
	chaosBHB := transport.WrapChaos(epBHB, transport.ChaosConfig{Up: true})

	nA := node.New("A", chaosA, "B", 200*time.Millisecond)
	nB := node.New("B", chaosB, "A", 200*time.Millisecond)
	nA.AttachHB(chaosAHB)
	nB.AttachHB(chaosBHB)

	nA.Start()
	nB.Start()
	defer nA.Stop()
	defer nB.Stop()
	defer chaosA.Close()
	defer chaosB.Close()
	defer chaosAHB.Close()
	defer chaosBHB.Close()

	var actA, actB model.ActorID
	copy(actA[:], bytes.Repeat([]byte{0xA1}, 16))
	copy(actB[:], bytes.Repeat([]byte{0xB2}, 16))

	nA.Put("x", []byte("v1"), actA)
	nB.Put("y", []byte("v2"), actB)

	time.Sleep(2 * time.Second)

	snapA := materialize.Snapshot(nA.Log)
	snapB := materialize.Snapshot(nB.Log)

	fmt.Printf("A state: %v\n", snapA)
	fmt.Printf("B state: %v\n", snapB)
}
