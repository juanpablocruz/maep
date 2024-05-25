package maep

import "github.com/juanpablocruz/maep/internal/maep/hlc"

func NewNode() *Node {
	n := &Node{}
	n.OperationMap = make(map[string]Operation)
	n.SyncVector = NewSyncVector()
	n.Clock = hlc.NewNow(0)

	return n
}
