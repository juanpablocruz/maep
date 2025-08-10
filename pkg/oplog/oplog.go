package oplog

import (
	"sync"

	"slices"

	"github.com/juanpablocruz/maep/pkg/model"
)

type Log struct {
	mu   sync.RWMutex
	data map[string][]model.Op
}

func New() *Log {
	return &Log{
		data: make(map[string][]model.Op),
	}
}

func (l *Log) Append(op model.Op) {
	l.mu.Lock()
	defer l.mu.Unlock()
	s := l.data[op.Key]
	for _, existing := range s {
		if existing.Hash == op.Hash {
			return // idempotent
		}
	}
	s = append(s, op)
	model.SortOpsMAEP(s)
	l.data[op.Key] = s
}

func (l *Log) Snapshot() map[string][]model.Op {
	l.mu.RLock()
	defer l.mu.RUnlock()
	out := make(map[string][]model.Op, len(l.data))
	for k, s := range l.data {
		cp := slices.Clone(s)
		out[k] = cp
	}
	return out
}

func (l *Log) CountPerKey() map[string]int {
	l.mu.RLock()
	defer l.mu.RUnlock()
	m := make(map[string]int, len(l.data))
	for k, s := range l.data {
		m[k] = len(s)
	}
	return m
}

// LastOp returns the last operation for a key (if any).
func (l *Log) LastOp(key string) (model.Op, bool) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	s := l.data[key]
	if len(s) == 0 {
		return model.Op{}, false
	}
	return s[len(s)-1], true
}
