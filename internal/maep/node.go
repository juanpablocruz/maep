package maep

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"log"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/juanpablocruz/maep/internal/maep/hlc"
	"github.com/juanpablocruz/maep/internal/network"
)

type Node struct {
	OperationMap map[string]Operation
	VersionTree  *VersionTree
	Block        *OperationBlock
	SyncVector   *SyncVector
	Clock        *hlc.Hybrid
	ctx          context.Context
	targetF      *string
	networkNode  *network.NetworkNode
}

type NodeConfig struct {
	Listen     *int
	Target     *string
	Standalone bool
}

func NewNode() *Node {
	n := &Node{}
	n.OperationMap = make(map[string]Operation)
	n.SyncVector = NewSyncVector()
	n.Clock = hlc.NewNow(0)
	return n
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

func (n *Node) FindHash(hash string, shorted bool) (*OperationBlock, []string, error) {
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
		if shorted {
			paths = append(paths, shortBlockId)
		} else {
			paths = append(paths, blockId)
		}
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
	_, paths, err := n.FindHash(hash, true)
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

func (n *Node) JoinNetwork(config NodeConfig) error {
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

	// Parse options from the command line

	n.networkNode = network.NewNetworkNode(config.Listen, config.Standalone)
	_, err := n.networkNode.SelectPeer(*config.Target)
	log.Printf("Selected peer: %s\n", n.networkNode.Peer.ID)
	return err
}

func (n *Node) Sync(transmitData []byte) error {
	log.Println("Syncing with network")

	n.networkNode.RunSender(transmitData, func(b []byte) ([]byte, error) {
		buf := bytes.NewBuffer(b)
		dec := gob.NewDecoder(buf)
		decodedVector := NewSyncVector()
		if err := dec.Decode(&decodedVector); err != nil {
			log.Println(err)
			return []byte{}, err
		}

		log.Printf("received sync vector: %v\n", decodedVector)
		return b, nil
	})

	return nil
}

func (n *Node) GetOperationsFromDiff(hash string) ([]Operation, error) {
	_, paths, err := n.FindHash(hash, false)
	if err != nil {
		return nil, err
	}

	ops := make([]Operation, 0)
	for _, path := range paths {
		op, ok := n.OperationMap[path]
		if ok {
			ops = append(ops, op)
		}
	}
	return ops, nil
}
