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
	o.mu.RLock()
	// Create a copy of the order slice to avoid holding the lock during iteration
	orderCopy := make([]OpLogEntry, len(o.order))
	copy(orderCopy, o.order)
	o.mu.RUnlock()

	low := frontier.Key
	zero := OpCannonicalKey{}
	if bytes.Equal(frontier.Key[:], zero[:]) {
		low = orderCopy[0].op.CanonicalKey()
	}

	return o.GetOpLogEntriesFrom(low, orderCopy[len(orderCopy)-1].op.CanonicalKey())
}

func (o *OpLog) GetOpLogEntriesFrom(low, high OpCannonicalKey) iter.Seq2[OpCannonicalKey, *OpLogEntry] {
	return func(yield func(OpCannonicalKey, *OpLogEntry) bool) {
		o.mu.RLock()
		// Create a copy of the order slice to avoid holding the lock during iteration
		orderCopy := make([]OpLogEntry, len(o.order))
		copy(orderCopy, o.order)
		o.mu.RUnlock()

		startAt := len(orderCopy)
		endAt := len(orderCopy)
		for i, entry := range orderCopy {
			if bytes.Equal(entry.key[:], low[:]) {
				startAt = i + 1
			}

			if bytes.Equal(entry.key[:], high[:]) {
				endAt = i
			}
		}

		if startAt > endAt {
			return
		}

		for i := startAt; i <= endAt; i++ {
			entry := orderCopy[i]
			if !yield(entry.key, &entry) {
				return
			}
		}
	}
}
