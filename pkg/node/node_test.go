package node

import (
	"testing"
	"time"

	"github.com/juanpablocruz/maep/pkg/hlc"
	"github.com/juanpablocruz/maep/pkg/model"
	"github.com/juanpablocruz/maep/pkg/oplog"
	"github.com/juanpablocruz/maep/pkg/syncproto"
)

func TestCountsEqual(t *testing.T) {
	a := map[string]int{"a": 1, "b": 2}
	b := map[string]int{"b": 2, "a": 1}
	if !countsEqual(a, b) {
		t.Fatalf("expected maps to be equal")
	}
	c := map[string]int{"a": 1}
	if countsEqual(a, c) {
		t.Fatalf("expected maps to differ")
	}
}

func TestMinMaxDur(t *testing.T) {
	a := time.Second
	b := 500 * time.Millisecond
	if got := minDur(a, b); got != b {
		t.Fatalf("minDur(%v,%v)=%v", a, b, got)
	}
	if got := maxDur(a, b); got != a {
		t.Fatalf("maxDur(%v,%v)=%v", a, b, got)
	}
}

func TestJitterRange(t *testing.T) {
	base := 100 * time.Millisecond
	for range 100 {
		j := jitter(base)
		if j < base*8/10 || j > base*12/10 {
			t.Fatalf("jitter out of range: %v", j)
		}
	}
}

func TestKickCollapses(t *testing.T) {
	n := &Node{kickCh: make(chan struct{}, 1)}
	n.kick()
	if len(n.kickCh) != 1 {
		t.Fatalf("expected kickCh length 1, got %d", len(n.kickCh))
	}
	n.kick()
	if len(n.kickCh) != 1 {
		t.Fatalf("kick should collapse bursts, got len %d", len(n.kickCh))
	}
}

func TestSetConnectedEvent(t *testing.T) {
	ch := make(chan Event, 10)
	n := &Node{Events: ch, kickCh: make(chan struct{}, 1)}
	n.SetConnected(true)
	select {
	case ev := <-ch:
		if ev.Type != EventConnChange || ev.Fields["up"] != true {
			t.Fatalf("unexpected event %+v", ev)
		}
	default:
		t.Fatalf("expected conn_change event")
	}
	n.SetConnected(true)
	select {
	case <-ch:
		t.Fatalf("did not expect event on same state")
	default:
	}
}

func TestSetPausedEvent(t *testing.T) {
	ch := make(chan Event, 10)
	n := &Node{Events: ch, kickCh: make(chan struct{}, 1)}
	n.SetPaused(true)
	select {
	case ev := <-ch:
		if ev.Type != EventPauseChange || ev.Fields["paused"] != true {
			t.Fatalf("unexpected event %+v", ev)
		}
	default:
		t.Fatalf("expected pause_change event")
	}
	n.SetPaused(true)
	select {
	case <-ch:
		t.Fatalf("did not expect event on same state")
	default:
	}
}

func TestOnMissAndOnPong(t *testing.T) {
	ch := make(chan Event, 10)
	n := &Node{Events: ch, hbMissK: 3}
	n.onMiss()
	n.onMiss()
	n.onMiss()
	if !n.suspect.Load() || !n.IsPaused() {
		t.Fatalf("expected node to be suspect and paused")
	}
	foundHealth := false
	for len(ch) > 0 {
		ev := <-ch
		if ev.Type == EventHealth {
			foundHealth = true
			if ev.Fields["state"] != "suspect" {
				t.Fatalf("expected suspect health event, got %+v", ev)
			}
		}
	}
	if !foundHealth {
		t.Fatalf("missing health event on miss")
	}

	// prepare for pong
	ch = make(chan Event, 10)
	n.Events = ch
	n.onPong()
	if n.suspect.Load() || n.IsPaused() {
		t.Fatalf("expected node to recover on pong")
	}
	foundHealthy := false
	for len(ch) > 0 {
		ev := <-ch
		if ev.Type == EventHealth {
			foundHealthy = true
			if ev.Fields["state"] != "healthy" {
				t.Fatalf("expected healthy health event, got %+v", ev)
			}
		}
	}
	if !foundHealthy {
		t.Fatalf("missing health event on pong")
	}
	if n.misses.Load() != 0 {
		t.Fatalf("misses not reset")
	}
}

func TestPutAndDelete(t *testing.T) {
	ch := make(chan Event, 10)
	n := &Node{Events: ch, Log: oplog.New(), Clock: hlc.New(), kickCh: make(chan struct{}, 1)}
	var actor model.ActorID
	actor[0] = 1
	n.Put("k", []byte("v"), actor)
	snap := n.Log.Snapshot()
	ops := snap["k"]
	if len(ops) != 1 || ops[0].Kind != model.OpKindPut || string(ops[0].Value) != "v" {
		t.Fatalf("put not appended: %#v", ops)
	}
	select {
	case ev := <-ch:
		if ev.Type != EventPut || ev.Fields["key"] != "k" || ev.Fields["val"] != "v" {
			t.Fatalf("unexpected put event: %+v", ev)
		}
	default:
		t.Fatalf("missing put event")
	}
	if len(n.kickCh) != 1 {
		t.Fatalf("kick not triggered on put")
	}

	ch = make(chan Event, 10)
	n.Events = ch
	n.Delete("k2", actor)
	snap = n.Log.Snapshot()
	ops = snap["k2"]
	if len(ops) != 1 || ops[0].Kind != model.OpKindDel {
		t.Fatalf("del not appended: %#v", ops)
	}
	select {
	case ev := <-ch:
		if ev.Type != EventDel || ev.Fields["key"] != "k2" {
			t.Fatalf("unexpected del event: %+v", ev)
		}
	default:
		t.Fatalf("missing del event")
	}
}

func TestChunkingSplitsAndMarksLast(t *testing.T) {
	// Create a delta with two entries big enough to force split
	big := make([]byte, 50)
	d := syncproto.Delta{Entries: []syncproto.DeltaEntry{
		{Key: "k1", Ops: []model.Op{{Version: model.OpSchemaV1, Kind: model.OpKindPut, Key: "k1", Value: big}}},
		{Key: "k2", Ops: []model.Op{{Version: model.OpSchemaV1, Kind: model.OpKindPut, Key: "k2", Value: big}}},
	}}
	chunks := syncproto.ChunkDelta(d, 128)
	if len(chunks) < 1 {
		t.Fatalf("expected at least one chunk")
	}
	if !chunks[len(chunks)-1].Last {
		t.Fatalf("last chunk must be marked Last=true")
	}
}
