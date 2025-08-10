package node

import (
	"time"

	"github.com/juanpablocruz/maep/pkg/hlc"
	"github.com/juanpablocruz/maep/pkg/oplog"
	"github.com/juanpablocruz/maep/pkg/protoport"
	"github.com/juanpablocruz/maep/pkg/transport"
)

// NodeOption configures a Node in NewWithOptions.
type NodeOption func(*Node)

func WithEndpoint(ep transport.EndpointIF) NodeOption {
	return func(n *Node) { n.EP = ep }
}
func WithPeer(peer transport.MemAddr) NodeOption {
	return func(n *Node) { n.Peer = peer }
}

// WithMessenger sets a custom typed messenger. Overrides WithEndpoint for send/recv.
func WithMessenger(ms protoport.Messenger) NodeOption { return func(n *Node) { n.ms = ms } }
func WithHeartbeatEndpoint(hb transport.EndpointIF) NodeOption {
	return func(n *Node) { n.HB = hb }
}
func WithTickerEvery(d time.Duration) NodeOption {
	return func(n *Node) { n.Ticker = d }
}
func WithDeltaMaxBytes(b int) NodeOption {
	return func(n *Node) { n.DeltaMaxBytes = b }
}
func WithDeltaWindowChunks(w int) NodeOption {
	return func(n *Node) { n.DeltaWindowChunks = w }
}
func WithRetransTimeout(d time.Duration) NodeOption {
	return func(n *Node) { n.RetransTimeout = d }
}
func WithDescent(enabled bool, leafK int) NodeOption {
	return func(n *Node) { n.DescentEnabled = enabled; n.DescentLeafK = leafK }
}
func WithBackoff(base, max time.Duration) NodeOption {
	return func(n *Node) { n.backoffBase = base; n.backoffMax = max }
}
func WithHB(every time.Duration, missK int) NodeOption {
	return func(n *Node) { n.hbEvery = every; n.hbMissK = missK }
}
func WithEvents(ch chan Event) NodeOption {
	return func(n *Node) { n.Events = ch }
}
func WithClock(c *hlc.Clock) NodeOption {
	return func(n *Node) { n.Clock = c }
}
func WithLog(l *oplog.Log) NodeOption {
	return func(n *Node) { n.Log = l }
}

// NewWithOptions constructs a Node with defaults and applies options.
func NewWithOptions(name string, opts ...NodeOption) *Node {
	n := New(name, nil, "", 250*time.Millisecond)
	for _, opt := range opts {
		if opt != nil {
			opt(n)
		}
	}
	return n
}
