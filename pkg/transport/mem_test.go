package transport

import (
	"context"
	"testing"
	"time"
)

func TestDelivery(t *testing.T) {
	sw := NewSwitch()
	a, err := sw.Listen("A")
	if err != nil {
		t.Fatal(err)
	}
	b, err := sw.Listen("B")
	if err != nil {
		t.Fatal(err)
	}

	msg := []byte("ping")
	if err := a.Send("B", msg); err != nil {
		t.Fatalf("send: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	got, ok := b.Recv(ctx)
	if !ok || string(got) != "ping" {
		t.Fatalf("recv mismatch: ok=%v got=%q", ok, got)
	}
}

func TestCloseStopsRecv(t *testing.T) {
	sw := NewSwitch()
	a, err := sw.Listen("A")
	if err != nil {
		t.Fatal(err)
	}

	a.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, ok := a.Recv(ctx); ok {
		t.Fatalf("expected closed recv to return ok=false")
	}
}
