// Package engine implements a distributed key-value store.
package engine

import (
	"bytes"
	"fmt"
	"sort"
	"sync"

	"github.com/juanpablocruz/maep/pkg/eventbus"
	"github.com/juanpablocruz/maep/pkg/fsm"
)

// OpsEventSubscriber is a Subscriber to EventBus that handles ops events.
type OpsEventSubscriber struct {
	ch   chan eventbus.Event
	wg   *sync.WaitGroup
	node *Node
}

func NewOpsEventSubscriber(node *Node) *OpsEventSubscriber {
	return &OpsEventSubscriber{
		node: node,
		ch:   make(chan eventbus.Event, 100),
		wg:   &sync.WaitGroup{},
	}
}

func (s *OpsEventSubscriber) GetChannel() chan eventbus.Event {
	return s.ch
}

func (s *OpsEventSubscriber) GetWaitGroup() *sync.WaitGroup {
	return s.wg
}

func (s *OpsEventSubscriber) OnEvent(event eventbus.Event) {
	if opEvent, ok := event.(*OpEvent); ok {
		_, isNew := s.node.addOp(opEvent.Op)
		if isNew {
			s.node.drainOnce()
		}
	}
}

type StateEventSubscriber struct {
	node *Node
	ch   chan eventbus.Event
	wg   *sync.WaitGroup
}

func NewStateEventSubscriber(node *Node) *StateEventSubscriber {
	return &StateEventSubscriber{
		node: node,
		ch:   make(chan eventbus.Event, 100),
		wg:   &sync.WaitGroup{},
	}
}

func (s *StateEventSubscriber) GetChannel() chan eventbus.Event {
	return s.ch
}

func (s *StateEventSubscriber) GetWaitGroup() *sync.WaitGroup {
	return s.wg
}

func (s *StateEventSubscriber) OnEvent(event eventbus.Event) {
	if e, ok := event.(*fsm.InitiatorStateEvent); ok {
		s.node.FSM.HandleInitiatorEvent(e)
	}

	if e, ok := event.(*fsm.ResponderStateEvent); ok {
		s.node.FSM.HandleResponderEvent(e)
	}
}

type Node struct {
	ID      PeerID
	SV      SV
	ops     *OpLog
	started bool
	mu      sync.RWMutex // Protect started field and other state

	frontier    Frontier
	subscribers []eventbus.Subscriber

	m   *Merkle
	HLC HLCTimestamp

	FSM *fsm.FSM
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

	opsLog := NewOpLog()
	node := &Node{
		ops: opsLog,
		m:   NewMerkle(*opts.MerkleFanout, *opts.MerkleDepth, opsLog.GetOpLogEntriesFrom),
		HLC: NewHLC(0, 0),
		SV:  make(SV),
		FSM: fsm.NewFSM(),
	}

	return node
}

func (n *Node) Start(bus *eventbus.EventBus) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.started {
		return
	}

	opsEventSubscriber := NewOpsEventSubscriber(n)
	fsmEventSubscriber := NewStateEventSubscriber(n)

	bus.Subscribe(opsEventSubscriber)
	bus.Subscribe(fsmEventSubscriber)

	n.subscribers = []eventbus.Subscriber{opsEventSubscriber, fsmEventSubscriber}

	// Start the eventBus to begin processing events
	bus.Start()

	n.started = true
}

func (n *Node) addOp(op *Op) (*OpLogEntry, bool) {
	return n.ops.Append(op)
}

func (n *Node) WaitForProcessing() {
	for _, s := range n.subscribers {
		s.GetWaitGroup().Wait()
	}
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
	for _, s := range n.subscribers {
		close(s.GetChannel())
	}
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

// PlanDeltas takes the set of differing leaves from the summary/descent and the peer's SV,
// scans the OpLog for each leaf's suffix after the peer's frontier, sorts the ops by key,
// and chunks into sendable batches.
func (n *Node) PlanDeltas(peer SummaryPeer, leaves []Prefix, maxOps int, maxBytes int) []DeltaPlan {
	var out []DeltaPlan

	// Get the peer's SV frontiers
	peerID := peer.GetID()
	peerFrontiers, exists := n.SV[peerID]
	if !exists {
		// If no SV exists for this peer, create an empty one
		peerFrontiers = []Frontier{}
	}

	// Create a map of peer frontiers by key for efficient lookup
	frontierMap := make(map[OpCannonicalKey]bool)
	for _, frontier := range peerFrontiers {
		if frontier.Set {
			frontierMap[frontier.Key] = true
		}
	}

	totalBytes := 0
	for _, leaf := range leaves {
		if len(out) >= maxOps {
			break
		}

		// Get the key range for this leaf prefix
		lowKey, highKey := n.getPrefixKeyBounds(leaf)

		// Collect operations in this leaf's range that are after the peer's frontier
		var leafOps []*OpLogEntry
		leafBytes := 0

		for key, entry := range n.ops.GetOpLogEntriesFrom(lowKey, highKey) {
			// Skip operations that are exactly at the peer's frontier
			if frontierMap[key] {
				continue
			}

			// Check if this operation is after ALL peer frontiers
			// An operation should be included if it's greater than ALL frontier keys
			if !n.isOperationAfterAllFrontiers(key, peerFrontiers) {
				continue
			}

			// Estimate the size of this operation
			opSize := n.estimateOperationSize(entry)

			// Check if adding this operation would exceed limits
			if len(leafOps) >= maxOps || (leafBytes+opSize) > maxBytes {
				break
			}

			leafOps = append(leafOps, entry)
			leafBytes += opSize
		}

		// If we found operations for this leaf, create a DeltaPlan
		if len(leafOps) > 0 {
			// Sort operations by canonical key to ensure deterministic order
			sort.Slice(leafOps, func(i, j int) bool {
				return bytes.Compare(leafOps[i].key[:], leafOps[j].key[:]) < 0
			})

			// Find the starting key for this delta plan
			fromK := lowKey
			if len(leafOps) > 0 {
				fromK = leafOps[0].key
			}

			plan := DeltaPlan{
				Leaf:  leaf,
				FromK: fromK,
				Ops:   leafOps,
			}

			out = append(out, plan)
			totalBytes += leafBytes
		}
	}

	return out
}

// getPrefixKeyBounds returns the lower and upper bounds for a given prefix
// The lower bound is the prefix itself, and the upper bound is the prefix with all remaining bits set to 1
func (n *Node) getPrefixKeyBounds(prefix Prefix) (OpCannonicalKey, OpCannonicalKey) {
	var lowKey, highKey OpCannonicalKey

	// Copy the prefix path to create the lower bound
	if len(prefix.Path) > 0 {
		copy(lowKey[:], prefix.Path)
	}

	// Create the upper bound by setting all remaining bits to 1
	copy(highKey[:], lowKey[:])

	// Calculate how many bits are used by the prefix
	bitsUsed := uint(prefix.Depth) * bitsPerLevel(n.m.Fanout)
	bytesUsed := bitsUsed / 8
	remainingBits := bitsUsed % 8

	// Set all remaining bits in the used bytes to 1
	for i := bytesUsed; i < uint(len(highKey)); i++ {
		highKey[i] = 0xFF
	}

	// If there are remaining bits in the last used byte, set them to 1
	if remainingBits > 0 && bytesUsed < uint(len(highKey)) {
		mask := byte(0xFF) << (8 - remainingBits)
		highKey[bytesUsed] |= mask
	}

	return lowKey, highKey
}

// isOperationAfterAllFrontiers checks if an operation is after all peer frontiers
func (n *Node) isOperationAfterAllFrontiers(key OpCannonicalKey, peerFrontiers []Frontier) bool {
	for _, frontier := range peerFrontiers {
		if frontier.Set && bytes.Compare(key[:], frontier.Key[:]) <= 0 {
			return false
		}
	}
	return true
}

// estimateOperationSize estimates the size of an operation in bytes
func (n *Node) estimateOperationSize(entry *OpLogEntry) int {
	return len(entry.op.Key) + len(entry.op.Value) + 60 // 60 bytes for canonical key overhead
}
