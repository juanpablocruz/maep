package engine

import (
	"bytes"
	"iter"
	"sort"
	"sync"
)

type OpLogEntry struct {
	hash OpHash
	op   *Op
	key  OpCannonicalKey
}

type OpLog struct {
	mu    sync.RWMutex // Protect concurrent access to Store and order
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
	o.mu.Lock()
	defer o.mu.Unlock()

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
	o.mu.RLock()
	defer o.mu.RUnlock()
	return o.Store[opHash]
}

func (o *OpLog) Len() int {
	o.mu.RLock()
	defer o.mu.RUnlock()
	return len(o.order)
}

// GetOrdered returns a deterministic total order of ops
func (o *OpLog) GetOrdered() iter.Seq2[OpCannonicalKey, *Op] {
	return func(yield func(OpCannonicalKey, *Op) bool) {
		o.mu.RLock()
		// Create a copy of the order slice to avoid holding the lock during iteration
		orderCopy := make([]OpLogEntry, len(o.order))
		copy(orderCopy, o.order)
		o.mu.RUnlock()

		for _, entry := range orderCopy {
			op := o.Get(entry.hash) // This will acquire its own read lock
			if !yield(entry.key, op) {
				return
			}
		}
	}
}

func (o *OpLog) GetFrom(frontier Frontier) iter.Seq2[OpCannonicalKey, *OpLogEntry] {
	return func(yield func(OpCannonicalKey, *OpLogEntry) bool) {
		o.mu.RLock()
		// Create a copy of the order slice to avoid holding the lock during iteration
		orderCopy := make([]OpLogEntry, len(o.order))
		copy(orderCopy, o.order)
		o.mu.RUnlock()

		startAt := 0
		if frontier.Set {
			for i, entry := range orderCopy {
				if bytes.Compare(entry.key[:], frontier.Key[:]) > 0 {
					startAt = i
					break
				}
			}
		}

		for _, entry := range orderCopy[startAt:] {
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

func (o *OpLog) GetOpLogEntriesFrom(low, high OpCannonicalKey) iter.Seq2[OpCannonicalKey, *OpLogEntry] {
	return func(yield func(OpCannonicalKey, *OpLogEntry) bool) {
		o.mu.RLock()
		// Create a copy of the order slice to avoid holding the lock during iteration
		orderCopy := make([]OpLogEntry, len(o.order))
		copy(orderCopy, o.order)
		o.mu.RUnlock()

		for _, entry := range orderCopy {
			if bytes.Compare(entry.key[:], low[:]) < 0 {
				continue
			}
			if bytes.Compare(entry.key[:], high[:]) > 0 {
				break
			}
			if !yield(entry.key, &entry) {
				return
			}
		}
	}
}
