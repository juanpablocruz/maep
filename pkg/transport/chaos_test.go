package transport

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestChaosUpDown(t *testing.T) {
	sw := NewSwitch()
	a, err := sw.Listen("A")
	if err != nil {
		t.Fatal(err)
	}
	b, err := sw.Listen("B")
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()

	chaos := WrapChaos(a, ChaosConfig{Up: false, Seed: 1})
	defer chaos.Close()

	if err := chaos.Send("B", []byte("hi")); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled when link down, got %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if _, ok := b.Recv(ctx); ok {
		t.Fatalf("expected no frame when link down")
	}
	if chaos.GetConfig().Up {
		t.Fatalf("GetConfig.Up should report false")
	}

	chaos.SetUp(true)
	if !chaos.GetConfig().Up {
		t.Fatalf("GetConfig.Up should report true after SetUp")
	}
	if err := chaos.Send("B", []byte("hi")); err != nil {
		t.Fatalf("send after SetUp: %v", err)
	}
	ctx2, cancel2 := context.WithTimeout(context.Background(), time.Second)
	defer cancel2()
	got, ok := b.Recv(ctx2)
	if !ok || string(got) != "hi" {
		t.Fatalf("recv mismatch: ok=%v got=%q", ok, got)
	}
}

func TestChaosDup(t *testing.T) {
	sw := NewSwitch()
	a, err := sw.Listen("A")
	if err != nil {
		t.Fatal(err)
	}
	b, err := sw.Listen("B")
	if err != nil {
		t.Fatal(err)
	}
	chaos := WrapChaos(a, ChaosConfig{Up: true, Dup: 1, Seed: 1})
	defer chaos.Close()
	defer b.Close()

	if err := chaos.Send("B", []byte("x")); err != nil {
		t.Fatalf("send: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	for i := 0; i < 2; i++ {
		got, ok := b.Recv(ctx)
		if !ok || string(got) != "x" {
			t.Fatalf("recv %d: ok=%v got=%q", i, ok, got)
		}
	}
	ctx2, cancel2 := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel2()
	if _, ok := b.Recv(ctx2); ok {
		t.Fatalf("expected only two frames")
	}
}

func TestChaosLoss(t *testing.T) {
	sw := NewSwitch()
	a, err := sw.Listen("A")
	if err != nil {
		t.Fatal(err)
	}
	b, err := sw.Listen("B")
	if err != nil {
		t.Fatal(err)
	}
	chaos := WrapChaos(a, ChaosConfig{Up: true, Loss: 1, Seed: 1})
	defer chaos.Close()
	defer b.Close()

	if err := chaos.Send("B", []byte("y")); err != nil {
		t.Fatalf("send: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if _, ok := b.Recv(ctx); ok {
		t.Fatalf("expected frame to be dropped")
	}
}

func TestChaosSendError(t *testing.T) {
	sw := NewSwitch()
	a, err := sw.Listen("A")
	if err != nil {
		t.Fatal(err)
	}
	chaos := WrapChaos(a, ChaosConfig{Up: true})
	defer chaos.Close()

	// No endpoint listening on B so Send should surface the error
	err = chaos.Send("B", []byte("z"))
	if err == nil {
		t.Fatalf("expected send error for unknown destination")
	}
}
