package engine

import (
	"crypto/rand"

	"github.com/juanpablocruz/maep/pkg/actor"
	"github.com/juanpablocruz/maep/pkg/hlc"
)

type OpOption func(*Op)

func WithActorID(act actor.ActorID) func(o *Op) {
	return func(o *Op) {
		o.Actor = act
	}
}

func WithHLC(h hlc.HLCTimestamp) func(o *Op) {
	return func(o *Op) {
		o.HLC = h
	}
}

func WithType(t OpType) func(o *Op) {
	return func(o *Op) {
		o.Type = t
	}
}

func WithKey(k []byte) func(o *Op) {
	return func(o *Op) {
		o.Key = k
	}
}

func WithValue(v []byte) func(o *Op) {
	return func(o *Op) {
		o.Value = v
	}
}

func generateOp(options ...OpOption) Op {
	actorID := actor.NewActor()
	h := hlc.NewWithPT(hlc.PT{})
	k := make([]byte, 16)
	rand.Read(k)

	v := make([]byte, 16)
	rand.Read(v)

	op := Op{
		Actor: actorID,
		HLC:   h.Now(),
		Type:  OpPut,
		Key:   k,
		Value: v,
	}

	for _, o := range options {
		o(&op)
	}

	return op
}

func generateOps(n int) []Op {
	ops := make([]Op, 0)
	for range n {
		o := generateOp()
		ops = append(ops, o)
	}
	return ops
}
