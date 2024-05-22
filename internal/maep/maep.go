package maep

func NewNode() *Node {
	n := &Node{}
	n.OperationMap = make(map[string][]Operation)
	return n
}
