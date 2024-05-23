package maep

import (
	"bytes"
	"crypto/sha256"
	"encoding/gob"
	"fmt"
	"time"

	"github.com/cbergoon/merkletree"
	"github.com/google/uuid"
)

type OperationBlock struct {
	PrevBlock *OperationBlock
	Id        string
	Hash      []byte
	Timestamp int64
}

type Operation struct {
	Arguments []byte
	Id        string
	Data      []string
	Timestamp int64
}

func NewOperation(args []byte, data []string) Operation {
	return Operation{
		Arguments: args,
		Data:      data,
		Timestamp: time.Now().Unix(),
		Id:        uuid.New().String(),
	}
}

type Node struct {
	OperationMap map[string][]Operation
	VersionTree  *VersionTree
	Block        *OperationBlock
}

func (on OperationBlock) CalculateHash() ([]byte, error) {
	return on.Hash, nil
}

func (on OperationBlock) Equals(other merkletree.Content) (bool, error) {
	otherONId := other.(OperationBlock).Id
	return on.Id == otherONId, nil
}

func (n *Node) AddOperation(ops []Operation) (*OperationBlock, error) {
	hasher := sha256.New()

	var b bytes.Buffer
	gob.NewEncoder(&b).Encode(ops)
	s := b.Bytes()

	_, err := hasher.Write(s)
	if err != nil {
		return nil, err
	}

	bc := hasher.Sum(nil)
	hash := fmt.Sprintf("%x", bc)
	n.OperationMap[hash] = ops

	ob := &OperationBlock{
		Id:        hash,
		Hash:      bc,
		PrevBlock: n.Block,
		Timestamp: time.Now().Unix(),
	}

	// recalculate the version tree
	err = n.AddLeaf(ob)
	if err != nil {
		return nil, err
	}

	return ob, nil
}

func (n *Node) AddLeaf(ob *OperationBlock) error {
	if n.VersionTree == nil {
		n.VersionTree = NewVersionTree(ob)
		return nil
	}

	n.Block = ob
	return n.VersionTree.AddLeaf(ob)
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

func (n *Node) FindHash(hash string) (*OperationBlock, []string, error) {
	block := n.Block
	paths := []string{}

	for block != nil {
		blockId := block.GetHash()
		shortBlockId := firstN(blockId, 7)

		if blockId == hash || shortBlockId == hash {
			return block, []string{}, nil
		}
		b, err := block.Next()
		if err != nil {
			return nil, []string{}, err
		}
		paths = append(paths, shortBlockId)
		block = b
	}
	return block, paths, nil
}
