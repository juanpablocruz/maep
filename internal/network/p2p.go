package network

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/google/uuid"
	"github.com/libp2p/go-libp2p"
)

// NetworkNode is a ring
type NetworkNode struct {
	next *NetworkNode
	Id   string
}

func NewNetworkNode() *NetworkNode {
	return &NetworkNode{
		Id: uuid.New().String(),
	}
}

func (n *NetworkNode) beginServer(address string) error {
	ch := make(chan os.Signal, 1)

	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	node, err := libp2p.New(
		libp2p.ListenAddrStrings(address),
	)
	if err != nil {
		return err
	}

	addr := node.Addrs()

	fmt.Printf("Node %s joined network at address %s\n", n.Id, addr)

	<-ch
	fmt.Println("\nShutting down...")
	if err := node.Close(); err != nil {
		return err
	}

	return nil
}

func (n *NetworkNode) Join(address string) error {
	return n.beginServer(address)
}

//
// Implement a P2P network of nodes.
// The process should be as follows:
// 1. When we launch the program, we have to provide the url of the network we want to join
// 2. The program will create æa new NetworkNode and send a request to the network to join
// 3. The network will respond  the list of nodes in the network
// 4. The program will create a new ree and request a sync
// 5. The network will begin the sync request
