package materialize

import (
	"slices"

	"github.com/juanpablocruz/maep/pkg/model"
	"github.com/juanpablocruz/maep/pkg/oplog"
)

type State struct {
	Present bool
	Value   []byte
	Last    model.Op
}

func Snapshot(l *oplog.Log) map[string]State {
	snap := l.Snapshot()
	out := make(map[string]State, len(snap))
	for k, ops := range snap {
		present, value, last := model.ReduceLWW(ops)
		var cp []byte
		if present && value != nil {
			cp = slices.Clone(value)
		}
		out[k] = State{Present: present, Value: cp, Last: last}
	}
	return out
}
