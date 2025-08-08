package model

func ReduceLWW(ops []Op) (present bool, value []byte, last Op) {
	if len(ops) == 0 {
		return false, nil, Op{}
	}
	last = ops[len(ops)-1]
	switch last.Kind {
	case OpKindDel:
		return false, nil, last
	case OpKindPut:
		return true, last.Value, last
	default:
		panic("unknown op kind")
	}
}
