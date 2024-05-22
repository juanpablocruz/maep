package network

// NetworkNode is a ring
type NetworkNode struct {
	next *NetworkNode
	Id   string
}

func (n *NetworkNode) Join() {
}

//
// Implement a P2P network of nodes.
// The process should be as follows:
// 1. When we launch the program, we have to provide the url of the network we want to join
// 2. The program will create a new NetworkNode and send a request to the network to join
// 3. The network will respond with the list of nodes in the network
// 4. The program will create a new VersionTree and request a sync
// 5. The network will begin the sync request
//
