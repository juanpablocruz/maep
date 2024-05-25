package maep

import (
	"crypto/sha256"
	"encoding/gob"
	"fmt"
	"slices"
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
	OperationMap map[string]Operation
	VersionTree  *VersionTree
	Block        *OperationBlock
	SyncVector   *SyncVector
	Clock        *hlc.Hybrid
}

func (on Operation) CalculateHash() ([]byte, error) {
	h := sha256.New()
	e := gob.NewEncoder(h)
	err := e.Encode(on)
	if err != nil {
		return nil, err
	}
	return h.Sum(nil), nil
}

func (on Operation) Equals(other merkletree.Content) (bool, error) {
	otherONId := other.(Operation).Id
	return on.Id == otherONId, nil
}

func (n *Node) AddOperation(ops Operation) (*OperationBlock, error) {
	// recalculate the version tree
	ob, err := n.AddLeaf(ops)
	if err != nil {
		return nil, err
	}

	n.OperationMap[ob.GetHash()] = ops
	return ob, nil
}

func (n *Node) AddLeaf(op Operation) (*OperationBlock, error) {
	if n.VersionTree == nil {
		n.VersionTree = NewVersionTree(op)
	}

	n.Clock.AddTicks(1)
	err := n.VersionTree.AddLeaf(op)
	if err != nil {
		return nil, err
	}
	newHash := n.VersionTree.Root()

	ob := &OperationBlock{
		Id:        uuid.New().String(),
		Hash:      newHash,
		PrevBlock: n.Block,
		Timestamp: time.Now().Unix(),
	}

	n.Block = ob
	return ob, nil
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
		shortBlockId := block.GetShortHash()

		if blockId == hash || shortBlockId == hash {
			return block, paths, nil
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

func (n *Node) GetDiff(hash string) (string, error) {
	_, paths, err := n.FindHash(hash)
	if err != nil {
		return "", err
	}
	str := make([]string, 0)
	block := n.Block

	if slices.Contains(paths, block.GetShortHash()) {
		str = append(str, fmt.Sprintf("\033[1;31m(HEAD) %s\033[0m", block.GetShortHash()))
	} else {
		str = append(str, fmt.Sprintf("(HEAD) %s", block.GetShortHash()))
	}

	block, err = block.Next()
	if err != nil {
		block = nil
	}

	padding := strings.Repeat(" ", 7)
	for block != nil {
		if slices.Contains(paths, block.GetShortHash()) {
			str = append(str, fmt.Sprintf("%s\033[1;31m%s\033[0m", padding, block.GetShortHash()))
		} else {
			str = append(str, fmt.Sprintf("%s%s", padding, block.GetShortHash()))
		}
		b, err := block.Next()
		if err != nil {
			block = nil
		} else {
			block = b
		}
	}
	return strings.Join(str, fmt.Sprintf("\n%s   | \n", padding)), nil
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
