// Package engine implements a distributed key-value store.
package engine

import (
	"fmt"
	"sync"

	"github.com/juanpablocruz/maep/pkg/eventbus"
)

// OpsEventSubscriber is a Subscriber to EventBus that handles ops events.
type OpsEventSubscriber struct {
	node *Node
}

func NewOpsEventSubscriber(node *Node) *OpsEventSubscriber {
	return &OpsEventSubscriber{
		node: node,
	}
}

func (s *OpsEventSubscriber) OnEvent(event eventbus.Event) {
	if opEvent, ok := event.(*OpEvent); ok {
		_, isNew := s.node.addOp(opEvent.Op)
		if isNew {
			s.node.drainOnce()
		}
	}
}

type Node struct {
	ID      PeerID
	SV      SV
	bus     *OpsEventSubscriber
	ops     *OpLog
	wg      sync.WaitGroup
	ch      chan eventbus.Event
	started bool
	mu      sync.RWMutex // Protect started field and other state

	frontier Frontier
	drainCh  chan struct{}
	drainWg  sync.WaitGroup

	m   *Merkle
	HLC HLCTimestamp
}

type NodeOptions struct {
	MerkleDepth  *uint8
	MerkleFanout *uint32
}

// DefaultNodeOptions returns a NodeOptions struct with default values
func DefaultNodeOptions() NodeOptions {
	defaultDepth := uint8(8)
	defaultFanout := uint32(4)
	return NodeOptions{
		MerkleDepth:  &defaultDepth,
		MerkleFanout: &defaultFanout,
	}
}

// NewNode creates a new Node with optional configuration
func NewNode(bus *eventbus.EventBus, opts *NodeOptions) *Node {
	// Use defaults if no options provided
	if opts == nil {
		defaultOpts := DefaultNodeOptions()
		opts = &defaultOpts
	}

	// Set defaults for any nil fields
	if opts.MerkleDepth == nil {
		defaultDepth := uint8(8)
		opts.MerkleDepth = &defaultDepth
	}
	if opts.MerkleFanout == nil {
		defaultFanout := uint32(4)
		opts.MerkleFanout = &defaultFanout
	}

	node := &Node{
		ops:     NewOpLog(),
		ch:      make(chan eventbus.Event),
		m:       NewMerkle(*opts.MerkleFanout, *opts.MerkleDepth),
		HLC:     NewHLC(0, 0),
		drainCh: make(chan struct{}, 100),
		SV:      make(SV),
	}
	subscriber := NewOpsEventSubscriber(node)
	node.bus = subscriber
	return node
}

func (n *Node) Start(bus *eventbus.EventBus) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.started {
		return
	}

	go func() {
		for event := range n.ch {
			n.bus.OnEvent(event)
			n.wg.Done()
		}
	}()
	s := eventbus.NewSubscriber(n.ch, &n.wg)
	bus.Subscribe(*s)

	n.started = true
}

func (n *Node) addOp(op *Op) (*OpLogEntry, bool) {
	return n.ops.Append(op)
}

func (n *Node) WaitForProcessing() {
	n.wg.Wait()
}

func (n *Node) drainOnce() {
	for _, entry := range n.ops.GetFrom(n.frontier) {
		n.m.Append(*entry)
		n.frontier = Frontier{Key: entry.key, Set: true}
	}
}

func (n *Node) OnSummaryReq(req SummaryReq) (SummaryResp, error) {
	fanout := n.m.Fanout
	depth := n.m.Depth

	kids, ok := n.m.Summarize(req.Prefix)
	if !ok {
		return SummaryResp{Peer: n.ID, Fanout: fanout, Depth: depth, Prefix: req.Prefix, Err: "invalid prefix"}, nil
	}

	return SummaryResp{
		Peer:     n.ID,
		Fanout:   fanout,
		Depth:    depth,
		Prefix:   req.Prefix,
		Children: kids,
		Root:     n.m.Root(),
	}, nil
}

func (n *Node) SendSummaryReq(req SummaryReq) (SummaryResp, error) {
	return n.OnSummaryReq(req)
}

func (n *Node) GetID() PeerID {
	return n.ID
}

// GetOpCount returns the number of operations in the OpLog
func (n *Node) GetOpCount() int {
	return n.ops.Len()
}

// GetMerkleRoot returns the current Merkle tree root
func (n *Node) GetMerkleRoot() MerkleHash {
	return n.m.Root()
}

// Stop stops the node processing
func (n *Node) Stop() {
	n.mu.Lock()
	defer n.mu.Unlock()

	if !n.started {
		return
	}
	close(n.ch)
	n.started = false
}

func (n *Node) SummaryRound(remote SummaryPeer) ([]Prefix, error) {
	// Ask root
	r, err := remote.SendSummaryReq(SummaryReq{Peer: n.ID, Prefix: Prefix{Depth: 0, Path: nil}, SV: n.SV[remote.GetID()]})
	if err != nil {
		return nil, err
	}
	if r.Err != "" {
		return nil, fmt.Errorf("remote error: %s", r.Err)
	}

	fanout := r.Fanout
	depth := r.Depth

	if r.Root == n.m.Root() {
		return nil, nil
	}

	// BFS/DFS over differing children using DescendDiff
	// repeatedly SendSummaryReq for deeper Prefixes
	// collect differing leaf Prefixes;
	// return the leaf Prefixes

	var out []Prefix
	stack := []Prefix{{Depth: 0, Path: nil}}

	for len(stack) > 0 {
		p := stack[len(stack)-1]
		stack = stack[:len(stack)-1]

		lkids, ok := n.m.Summarize(p)
		if !ok {
			return nil, fmt.Errorf("invalid local prefix: %s, depth: %d", p.Path, p.Depth)
		}

		rr, err := remote.SendSummaryReq(SummaryReq{Peer: n.ID, Prefix: p, SV: n.SV[remote.GetID()]})
		if err != nil {
			return nil, err
		}
		if rr.Err != "" {
			return nil, fmt.Errorf("remote error: %s", rr.Err)
		}

		if eqKids(lkids, rr.Children) {
			// Even if children are equal, if roots are different, we need to traverse deeper
			// But only if we haven't reached the maximum depth
			if p.Depth+1 < depth {
				for i := range fanout {
					stack = append(stack, prefixExtend(p, int(i), fanout))
				}
			} else {
				// At maximum depth, add all children as leaf prefixes since roots are different
				for i := range fanout {
					out = append(out, prefixExtend(p, int(i), fanout))
				}
			}
			continue
		}

		if p.Depth+1 == depth {
			for i := range fanout {
				if lkids[i] != rr.Children[i] {
					out = append(out, prefixExtend(p, int(i), fanout))
				}
			}
			continue
		}

		for i := range fanout {
			if lkids[i] != rr.Children[i] {
				stack = append(stack, prefixExtend(p, int(i), fanout))
			}
		}
	}

	return out, nil
}
