package network

import (
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/google/uuid"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	pNetwork "github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/peerstore"
	ma "github.com/multiformats/go-multiaddr"
)

// NetworkNode is a ring
type NetworkNode struct {
	host         host.Host
	ctx          context.Context
	Peer         *peer.AddrInfo
	closeContext context.CancelFunc
	Id           string
	Listen       int
}

func NewNetworkNode(listenF *int, standalone bool) *NetworkNode {
	n := &NetworkNode{
		Id: uuid.New().String(),
	}

	if standalone {
		return n
	}

	ctx, cancel := context.WithCancel(context.Background())
	n.closeContext = cancel

	// LibP2P code uses golog to log messages. They log with different
	// string IDs (i.e. "swarm"). We can control the verbosity level for
	// all loggers with:
	n.ctx = ctx

	if *listenF == 0 {
		log.Fatal("Please provide a port to bind on with -l")
	}
	n.Listen = *listenF
	// Make a host that listens on the given multiaddress
	_, err := n.MakeBasicHost(*listenF)
	if err != nil {
		log.Fatal(err)
	}

	return n
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

func (n *NetworkNode) SelectPeer(targetPeer string) (*peer.AddrInfo, error) {
	fullAddr := getHostAddress(n.host)
	log.Printf("I am %s\n", fullAddr)

	// Turn the targetPeer into a multiaddr.
	maddr, err := ma.NewMultiaddr(targetPeer)
	if err != nil {
		return nil, err
	}

	// Extract the peer ID from the multiaddr.
	info, err := peer.AddrInfoFromP2pAddr(maddr)
	if err != nil {
		return nil, err
	}

	// We have a peer ID and a targetAddr, so we add it to the peerstore
	// so LibP2P knows how to contact it
	n.host.Peerstore().AddAddrs(info.ID, info.Addrs, peerstore.PermanentAddrTTL)

	n.Peer = info
	return info, nil
}

//
// Implement a P2P network of nodes.
// The process should be as follows:
// 1. When we launch the program, we have to provide the url of the network we want to join
// 2. The program will create æa new NetworkNode and send a request to the network to join
// 3. The network will respond  the list of nodes in the network
// 4. The program will create a new ree and request a sync
// 5. The network will begin the sync request
//
//

// makeBasicHost creates a LibP2P host with a random peer ID listening on the
// given multiaddress. It won't encrypt the connection if insecure is true.
func (n *NetworkNode) MakeBasicHost(listenPort int) (host.Host, error) {
	r := rand.Reader

	// Generate a key pair for this host. We will use it at least
	// to obtain a valid host ID.
	priv, _, err := crypto.GenerateKeyPairWithReader(crypto.RSA, 2048, r)
	if err != nil {
		return nil, err
	}

	opts := []libp2p.Option{
		libp2p.ListenAddrStrings(fmt.Sprintf("/ip4/127.0.0.1/tcp/%d", listenPort)),
		libp2p.Identity(priv),
		libp2p.DisableRelay(),
	}

	h, err := libp2p.New(opts...)
	if err != nil {
		return nil, err
	}
	n.host = h
	return h, nil
}

func getHostAddress(ha host.Host) string {
	// Build host multiaddress
	hostAddr, _ := ma.NewMultiaddr(fmt.Sprintf("/p2p/%s", ha.ID()))

	// Now we can build a full multiaddress to reach this host
	// by encapsulating both addresses:
	addr := ha.Addrs()[0]
	return addr.Encapsulate(hostAddr).String()
}

func (n *NetworkNode) StartListener(ctx context.Context, callback func(s pNetwork.Stream) error) {
	fullAddr := getHostAddress(n.host)
	log.Printf("I am %s\n", fullAddr)

	// Set a stream handler on host A. /echo/1.0.0 is
	// a user-defined protocol name.
	n.host.SetStreamHandler("/echo/1.0.0", func(s pNetwork.Stream) {
		log.Println("listener received new stream")
		if err := callback(s); err != nil {
			log.Println(err)
			s.Reset()
		} else {
			s.Close()
		}
	})

	log.Println("listening for connections")
	log.Printf("Now run \"go run ./cmd/maep -l %d -d %s\" on a different terminal\n", n.Listen+1, fullAddr)
}

func (n *NetworkNode) RunSender(transmitChannel []byte, convert func([]byte) ([]byte, error)) {
	log.Println("sender opening stream")
	defer n.ctx.Done()
	// make a new stream from host B to host A
	// it should be handled on host A by the handler we set above because
	// we use the same /echo/1.0.0 protocol
	s, err := n.host.NewStream(context.Background(), n.Peer.ID, "/echo/1.0.0")
	if err != nil {
		log.Println(err)
		return
	}

	log.Println("sender saying hello")
	data := transmitChannel
	_, err = s.Write(data)
	if err != nil {
		log.Println(err)
		return
	}

	out, err := io.ReadAll(s)
	if err != nil {
		log.Println(err)
		return
	}

	convert(out)

	log.Printf("read reply: %q\n", out)
}
