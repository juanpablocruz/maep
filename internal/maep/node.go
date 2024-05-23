package maep

import (
	"bytes"
	"crypto/sha256"
	"encoding/gob"
	"fmt"
	"strings"
	"time"

	"github.com/cbergoon/merkletree"
	"github.com/google/uuid"
	"github.com/juanpablocruz/maep/internal/maep/hlc"
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
	SyncVector   *SyncVector
	Clock        *hlc.Hybrid
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
	}

	n.Block = ob
	n.Clock.AddTicks(1)
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

func (n *Node) Print() string {
	str := make([]string, 0)
	block := n.Block

	str = append(str, fmt.Sprintf("\033[1;32m(HEAD) %s\033[0m", block.GetShortHash()))

	block, err := block.Next()
	if err != nil {
		block = nil
	}

	padding := strings.Repeat(" ", 7)
	for block != nil {
		str = append(str, fmt.Sprintf("%s%s", padding, block.GetShortHash()))
		b, err := block.Next()
		if err != nil {
			block = nil
		} else {
			block = b
		}
	}
	return strings.Join(str, fmt.Sprintf("\n%s   | \n", padding))
}

func (n *Node) JoinNetwork() error {
	// Call the seed node to retrieve the the target node ip address and port
	//
	// Connect to the target node
	// Send a message to the target node to request join
	//
	// This nodes needs to send to target node its SyncVector and VersionTree,
	// the target node will then sync with this node and send its SyncVector and VersionTree
	//
	// The target node will set its SyncVector.NextBlock to the new node's ID
	// This node will set its SyncVector.NextBlock to previous target node's SyncVector.NextBlock
	// This will join this node to the ring network

	return nil
}

func (n *Node) Sync() error {
	return nil
}
