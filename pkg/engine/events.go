package engine

type OpEvent struct {
	Op *Op
}

func (e *OpEvent) GetType() string {
	return "OpEvent"
}
