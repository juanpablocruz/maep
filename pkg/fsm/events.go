package fsm

type InitiatorStateEvent struct {
}

type ResponderStateEvent struct {
}

func (e *InitiatorStateEvent) GetType() string {
	return "InitiatorStateEvent"
}

func (e *ResponderStateEvent) GetType() string {
	return "InitiatorStateEvent"
}
