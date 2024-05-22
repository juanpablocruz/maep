package maep

import (
	"bytes"
	"crypto/sha256"
	"encoding/gob"
	"time"

	"github.com/cbergoon/merkletree"
)

type OperationNode struct {
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

type Node struct {
	OperationMap map[string][]Operation
	VersionTree  *VersionTree
}

func (on OperationNode) CalculateHash() ([]byte, error) {
	return on.Hash, nil
}

func (on OperationNode) Equals(other merkletree.Content) (bool, error) {
	otherONId := other.(OperationNode).Id
	return on.Id == otherONId, nil
}

func (n *Node) AddOperation(ops []Operation) (string, error) {
	hasher := sha256.New()

	var b bytes.Buffer
	gob.NewEncoder(&b).Encode(ops)
	s := b.Bytes()

	_, err := hasher.Write(s)
	if err != nil {
		return "", err
	}
	bc := hasher.Sum(nil)
	hash := string(bc)
	n.OperationMap[hash] = ops

	on := &OperationNode{
		Id:        hash,
		Hash:      bc,
		Timestamp: time.Now().Unix(),
	}

	// recalculate the version tree
	err = n.AddLeaf(on)
	if err != nil {
		return "", err
	}

	return hash, nil
}

func (n *Node) AddLeaf(on *OperationNode) error {
	if n.VersionTree == nil {
		n.VersionTree = NewVersionTree(on)
		return nil
	}

	return n.VersionTree.AddLeaf(on)
}
