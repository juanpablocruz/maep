package model

func ReduceLWW(ops []Op) (present bool, value []byte, last Op) {
	if len(ops) == 0 {
		return false, nil, Op{}
	}
	last = ops[len(ops)-1] // PRECONDITION: ops sorted by SortOpsMAEP
	switch last.Kind {
	case OpKindDel:
		return false, nil, last
	case OpKindPut:
		// return a copy to avoid aliasing
		cp := make([]byte, len(last.Value))
		copy(cp, last.Value)
		return true, cp, last
	default:
		panic("unknown op kind")
	}
}
