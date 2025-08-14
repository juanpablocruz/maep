package engine

import (
	"bytes"
	"iter"
	"sort"
)

type OpLogEntry struct {
	hash OpHash
	op   *Op
	key  OpCannonicalKey
}

type OpLog struct {
	Store map[OpHash]*Op
	order []OpLogEntry
}

func NewOpLog() *OpLog {
	return &OpLog{
		Store: make(map[OpHash]*Op),
		order: []OpLogEntry{},
	}
}

func (o *OpLog) Append(op *Op) (*OpLogEntry, bool) {
	opHash := op.Hash()
	// Append must be idempotent
	if _, ok := o.Store[opHash]; ok {
		var entry OpLogEntry
		for _, e := range o.order {
			if e.hash == opHash {
				entry = e
				break
			}
		}
		return &entry, false
	}
	entry := OpLogEntry{
		hash: opHash,
		op:   op,
		key:  op.CanonicalKey(),
	}

	o.Store[entry.hash] = op

	// Append in order sorted by Key
	o.order = append(o.order, entry)
	sort.Slice(o.order, func(i, j int) bool {
		return bytes.Compare(o.order[i].key[:], o.order[j].key[:]) < 0
	})
	// true means it is new
	return &entry, true
}

func (o *OpLog) Get(opHash OpHash) *Op {
	return o.Store[opHash]
}

func (o *OpLog) Len() int {
	return len(o.order)
}

// GetOrdered returns a deterministic total order of ops
func (o *OpLog) GetOrdered() iter.Seq2[OpCannonicalKey, *Op] {
	return func(yield func(OpCannonicalKey, *Op) bool) {
		for _, entry := range o.order {
			op := o.Store[entry.op.Hash()]
			if !yield(entry.key, op) {
				return
			}
		}
	}
}

func (o *OpLog) GetFrom(frontier Frontier) iter.Seq2[OpCannonicalKey, *OpLogEntry] {
	return func(yield func(OpCannonicalKey, *OpLogEntry) bool) {

		startAt := 0
		if frontier.Set {
			for i, entry := range o.order {
				if bytes.Compare(entry.key[:], frontier.Key[:]) > 0 {
					startAt = i
					break
				}
			}
		}

		for _, entry := range o.order[startAt:] {
			// Skip the entry that matches the frontier key exactly
			// BUG: this should not be like this, but it's a quick fix
			if frontier.Set && bytes.Equal(entry.key[:], frontier.Key[:]) {
				continue
			}
			if !yield(entry.key, &entry) {
				return
			}
		}
	}
}
