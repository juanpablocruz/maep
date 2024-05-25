package maep

import "fmt"

type OperationBlock struct {
	PrevBlock *OperationBlock
	Id        string
	Hash      []byte
	Timestamp int64
}

func (n *OperationBlock) Next() (*OperationBlock, error) {
	return n.PrevBlock, nil
}

func (ob *OperationBlock) GetHash() string {
	return fmt.Sprintf("%x", ob.Hash)
}

func (ob *OperationBlock) GetShortHash() string {
	return firstN(fmt.Sprintf("%x", ob.GetHash()), 7)
}
