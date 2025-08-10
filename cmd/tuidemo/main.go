package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"log/slog"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/charmbracelet/bubbles/table"
	"github.com/charmbracelet/bubbles/viewport"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"

	"github.com/juanpablocruz/maep/pkg/materialize"
	"github.com/juanpablocruz/maep/pkg/merkle"
	"github.com/juanpablocruz/maep/pkg/model"
	"github.com/juanpablocruz/maep/pkg/node"
	"github.com/juanpablocruz/maep/pkg/transport"
)

// ============================================================================
// Configuration and Constants
// ============================================================================

const (
	// Default values
	defaultNodes         = 3
	defaultWriteInterval = 3 * time.Second
	defaultSyncInterval  = 7 * time.Second
	defaultHeartbeat     = 750 * time.Millisecond
	defaultSuspectTO     = 2250 * time.Millisecond
	defaultDeltaChunk    = 64 * 1024
	defaultBasePort      = 9001

	// Writer constants
	writerKeys = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
)

// ============================================================================
// Configuration
// ============================================================================

type Config struct {
	Nodes         int
	WriteInterval time.Duration
	SyncInterval  time.Duration
	Heartbeat     time.Duration
	SuspectTO     time.Duration
	DeltaChunk    int
	BasePort      int

	// Chaos network parameters
	Loss   float64
	Dup    float64
	Reord  float64
	Delay  time.Duration
	Jitter time.Duration

	// Failure simulation
	FailurePeriod time.Duration
	RecoveryDelay time.Duration

	// Protocol tuning
	DeltaWindowChunks int
	RetransTimeout    time.Duration
	Descent           bool
	Auto              bool
}

func parseFlags() *Config {
	cfg := &Config{}

	flag.IntVar(&cfg.Nodes, "nodes", defaultNodes, "number of nodes in ring")
	flag.DurationVar(&cfg.WriteInterval, "write-interval", defaultWriteInterval, "mean interval between local writes per node")
	flag.DurationVar(&cfg.SyncInterval, "sync-interval", defaultSyncInterval, "period between sync attempts")
	flag.DurationVar(&cfg.Heartbeat, "heartbeat", defaultHeartbeat, "heartbeat period")
	flag.DurationVar(&cfg.SuspectTO, "suspect-timeout", defaultSuspectTO, "failure-suspect threshold (>=3*heartbeat)")
	flag.IntVar(&cfg.DeltaChunk, "delta-chunk-bytes", defaultDeltaChunk, "max bytes per SYNC_DELTA frame")
	flag.IntVar(&cfg.BasePort, "base-port", defaultBasePort, "base TCP/UDP port (node i uses base+i)")

	// Chaos knobs
	flag.Float64Var(&cfg.Loss, "loss", 0.0, "drop probability [0..1]")
	flag.Float64Var(&cfg.Dup, "dup", 0.0, "dup probability [0..1]")
	flag.Float64Var(&cfg.Reord, "reorder", 0.0, "reorder probability [0..1]")
	flag.DurationVar(&cfg.Delay, "delay", 0, "base one-way delay")
	flag.DurationVar(&cfg.Jitter, "jitter", 0, "jitter (+/-)")

	// Failure simulation
	flag.DurationVar(&cfg.FailurePeriod, "failure-period", 0, "mean time between random link failures (0=off)")
	flag.DurationVar(&cfg.RecoveryDelay, "recovery-delay", 5*time.Second, "time a failed link stays down before reconnect")

	// Protocol tuning
	flag.IntVar(&cfg.DeltaWindowChunks, "delta-window-chunks", 1, "max delta chunks in flight per session")
	flag.DurationVar(&cfg.RetransTimeout, "retrans-timeout", 2*time.Second, "retransmit timeout for unacked chunks")
	flag.BoolVar(&cfg.Descent, "descent", true, "use Merkle descent for summaries")
	flag.BoolVar(&cfg.Auto, "auto", false, "enable automatic writers")

	flag.Parse()
	return cfg
}

func (cfg *Config) Validate() error {
	if cfg.Nodes < 2 {
		return fmt.Errorf("need at least 2 nodes, got %d", cfg.Nodes)
	}
	if cfg.Loss < 0 || cfg.Loss > 1 {
		return fmt.Errorf("loss must be between 0 and 1, got %f", cfg.Loss)
	}
	if cfg.Dup < 0 || cfg.Dup > 1 {
		return fmt.Errorf("dup must be between 0 and 1, got %f", cfg.Dup)
	}
	if cfg.Reord < 0 || cfg.Reord > 1 {
		return fmt.Errorf("reorder must be between 0 and 1, got %f", cfg.Reord)
	}
	if cfg.Delay < 0 {
		return fmt.Errorf("delay cannot be negative, got %v", cfg.Delay)
	}
	if cfg.Jitter < 0 {
		return fmt.Errorf("jitter cannot be negative, got %v", cfg.Jitter)
	}

	return nil
}

// ============================================================================
// Node Management
// ============================================================================

type SimNode struct {
	Name     string
	TCP, UDP *transport.ChaosEP
	PeerAddr transport.MemAddr
	Node     *node.Node
	Actor    model.ActorID
	Port     int
}

type NodeManager struct {
	nodes  []*SimNode
	cfg    *Config
	mu     sync.RWMutex
	nextID int
}

func NewNodeManager(cfg *Config) *NodeManager {
	return &NodeManager{
		nodes:  make([]*SimNode, 0, cfg.Nodes),
		cfg:    cfg,
		nextID: 0,
	}
}

func (nm *NodeManager) CreateNodes() error {
	nm.mu.Lock()
	defer nm.mu.Unlock()

	// Create initial nodes
	for i := 0; i < nm.cfg.Nodes; i++ {
		if err := nm.addNode(); err != nil {
			return fmt.Errorf("failed to create node %d: %w", i, err)
		}
	}

	return nil
}

func (nm *NodeManager) addNode() error {
	index := nm.nextID
	nm.nextID++

	node, err := nm.createNode(index)
	if err != nil {
		return fmt.Errorf("failed to create node %d: %w", index, err)
	}

	nm.nodes = append(nm.nodes, node)

	// Update ring topology
	nm.updateRingTopology()

	return nil
}

func (nm *NodeManager) removeNode(index int) error {
	nm.mu.Lock()
	defer nm.mu.Unlock()

	if index < 0 || index >= len(nm.nodes) {
		return fmt.Errorf("invalid node index: %d", index)
	}

	// Stop and cleanup the node
	node := nm.nodes[index]
	node.Node.Stop()
	node.TCP.Close()
	node.UDP.Close()

	// Remove from slice
	nm.nodes = append(nm.nodes[:index], nm.nodes[index+1:]...)

	// Update ring topology
	nm.updateRingTopology()

	return nil
}

func (nm *NodeManager) updateRingTopology() {
	if len(nm.nodes) < 2 {
		return
	}

	// Recreate all nodes with correct peer addresses
	for i := range nm.nodes {
		next := (i + 1) % len(nm.nodes)
		// Use the peer node's actual listening port directly
		peerAddr := transport.MemAddr(fmt.Sprintf("127.0.0.1:%d", nm.nodes[next].Port))
		nm.nodes[i].PeerAddr = peerAddr

		// Recreate the node with the correct peer address
		nm.nodes[i].Node.Stop()
		newNode := node.NewWithOptions(
			nm.nodes[i].Name,
			node.WithEndpoint(nm.nodes[i].TCP),
			node.WithPeer(peerAddr),
			node.WithTickerEvery(nm.cfg.SyncInterval),
		)
		newNode.AttachHB(nm.nodes[i].UDP)
		newNode.SetDeltaMaxBytes(nm.cfg.DeltaChunk)
		newNode.SetHeartbeatEvery(nm.cfg.Heartbeat)
		hbK := max(int((nm.cfg.SuspectTO+nm.cfg.Heartbeat-1)/nm.cfg.Heartbeat), 3)
		newNode.SetSuspectThreshold(hbK)
		newNode.SetDeltaWindowChunks(nm.cfg.DeltaWindowChunks)
		newNode.SetRetransTimeout(nm.cfg.RetransTimeout)
		newNode.DescentEnabled = nm.cfg.Descent
		// IMPORTANT: attach events channel so protocol emits
		newNode.AttachEvents(make(chan node.Event, 1024))
		nm.nodes[i].Node = newNode
	}
}

func (nm *NodeManager) createNode(index int) (*SimNode, error) {
	port := nm.cfg.BasePort + index

	// Create TCP endpoint
	tcp, err := transport.ListenTCP(fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		return nil, fmt.Errorf("failed to create TCP endpoint: %w", err)
	}

	// Create UDP endpoint
	udp, err := transport.ListenUDP(fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		tcp.Close()
		return nil, fmt.Errorf("failed to create UDP endpoint: %w", err)
	}

	// Wrap with chaos
	chaosTCP := transport.WrapChaos(tcp, transport.ChaosConfig{Up: true})
	chaosUDP := transport.WrapChaos(udp, transport.ChaosConfig{Up: true})

	// Configure chaos parameters
	nm.configureChaos(chaosTCP, chaosUDP)

	// Create node with temporary peer address (will be set properly after ring setup)
	n := node.NewWithOptions(
		fmt.Sprintf("%d", index),
		node.WithEndpoint(chaosTCP),
		node.WithPeer(transport.MemAddr("127.0.0.1:9999")),
		node.WithTickerEvery(nm.cfg.SyncInterval),
	)
	n.AttachHB(chaosUDP)
	n.SetDeltaMaxBytes(nm.cfg.DeltaChunk)
	n.SetHeartbeatEvery(nm.cfg.Heartbeat)
	hbK := max(int((nm.cfg.SuspectTO+nm.cfg.Heartbeat-1)/nm.cfg.Heartbeat), 3)
	n.SetSuspectThreshold(hbK)
	n.SetDeltaWindowChunks(nm.cfg.DeltaWindowChunks)
	n.SetRetransTimeout(nm.cfg.RetransTimeout)
	n.DescentEnabled = nm.cfg.Descent
	// IMPORTANT: attach events channel so protocol emits
	n.AttachEvents(make(chan node.Event, 1024))

	// Create actor ID
	var actor model.ActorID
	copy(actor[:], bytes.Repeat([]byte{byte(0xA0 + index)}, 16))

	return &SimNode{
		Name:  fmt.Sprintf("%d", index),
		TCP:   chaosTCP,
		UDP:   chaosUDP,
		Node:  n,
		Actor: actor,
		Port:  port,
	}, nil
}

func (nm *NodeManager) configureChaos(tcp, udp *transport.ChaosEP) {
	tcp.SetLoss(nm.cfg.Loss)
	tcp.SetDup(nm.cfg.Dup)
	tcp.SetReorder(nm.cfg.Reord)
	tcp.SetBaseDelay(nm.cfg.Delay)
	tcp.SetJitter(nm.cfg.Jitter)

	udp.SetLoss(nm.cfg.Loss)
	udp.SetDup(nm.cfg.Dup)
	udp.SetReorder(nm.cfg.Reord)
	udp.SetBaseDelay(nm.cfg.Delay)
	udp.SetJitter(nm.cfg.Jitter)
}

func (nm *NodeManager) StartNodes() error {
	nm.mu.RLock()
	defer nm.mu.RUnlock()

	for _, node := range nm.nodes {
		// Start the node (peer address is already set in constructor)
		node.Node.Start()
	}

	// Add initial seed data
	if len(nm.nodes) > 0 {
		nm.nodes[0].Node.Put("A", []byte("v1"), nm.nodes[0].Actor)
	}

	return nil
}

func (nm *NodeManager) StopNodes() {
	nm.mu.RLock()
	defer nm.mu.RUnlock()

	log.Println("Stopping all nodes...")
	for _, node := range nm.nodes {
		log.Printf("Stopping node %s...", node.Name)

		// Stop the node first
		node.Node.Stop()

		// Close TCP endpoint
		log.Printf("Closing TCP endpoint for %s...", node.Name)
		node.TCP.Close()

		// Close UDP endpoint
		log.Printf("Closing UDP endpoint for %s...", node.Name)
		node.UDP.Close()

		log.Printf("Node %s stopped successfully", node.Name)
	}
	log.Println("All nodes stopped.")
}

func (nm *NodeManager) GetNodes() []*SimNode {
	nm.mu.RLock()
	defer nm.mu.RUnlock()

	result := make([]*SimNode, len(nm.nodes))
	copy(result, nm.nodes)
	return result
}

func (nm *NodeManager) GetNode(index int) *SimNode {
	nm.mu.RLock()
	defer nm.mu.RUnlock()

	if index >= 0 && index < len(nm.nodes) {
		return nm.nodes[index]
	}
	return nil
}

func (nm *NodeManager) AddNode() error {
	nm.mu.Lock()
	defer nm.mu.Unlock()

	return nm.addNode()
}

func (nm *NodeManager) RemoveNode(index int) error {
	return nm.removeNode(index)
}

// ============================================================================
// Event Management
// ============================================================================

type EventManager struct {
	events   []node.Event
	wireLog  []node.Event
	cmdLog   []node.Event
	mu       sync.RWMutex
	eventCh  chan node.Event
	capacity int
	svByNode map[string]struct {
		Segs int
		Keys int
		Top  []string
	}
}

func NewEventManager(capacity int) *EventManager {
	return &EventManager{
		events:   make([]node.Event, 0, capacity),
		wireLog:  make([]node.Event, 0, capacity),
		cmdLog:   make([]node.Event, 0, 256),
		eventCh:  make(chan node.Event, capacity),
		capacity: capacity,
		svByNode: make(map[string]struct {
			Segs int
			Keys int
			Top  []string
		}),
	}
}

func (em *EventManager) AddEvent(event node.Event) {
	em.mu.Lock()
	defer em.mu.Unlock()

	if event.Type == node.EventHB {
		return // Skip heartbeat events to reduce noise
	}

	// route command echoes to separate log
	if string(event.Type) == "cmd" {
		em.cmdLog = append(em.cmdLog, event)
		if len(em.cmdLog) > 200 {
			em.cmdLog = em.cmdLog[len(em.cmdLog)-200:]
		}
		return
	}

	if event.Type == node.EventSendSummary {
		// Capture SV snapshot from summary events (either dir send or recv)
		segs, _ := event.Fields["sv_segs"].(int)
		keys, _ := event.Fields["sv_keys"].(int)
		var top []string
		if v, ok := event.Fields["top_segs"].([]string); ok {
			top = v
		}
		em.svByNode[event.Node] = struct {
			Segs int
			Keys int
			Top  []string
		}{Segs: segs, Keys: keys, Top: top}
	}

	if event.Type == node.EventWire {
		em.wireLog = append(em.wireLog, event)
		if len(em.wireLog) > 50 {
			em.wireLog = em.wireLog[len(em.wireLog)-50:]
		}
	} else {
		em.events = append(em.events, event)
		if len(em.events) > em.capacity {
			em.events = em.events[len(em.events)-em.capacity:]
		}
	}
}

func (em *EventManager) GetEvents() []node.Event {
	em.mu.RLock()
	defer em.mu.RUnlock()

	result := make([]node.Event, len(em.events))
	copy(result, em.events)
	return result
}

func (em *EventManager) GetWireLog() []node.Event {
	em.mu.RLock()
	defer em.mu.RUnlock()

	result := make([]node.Event, len(em.wireLog))
	copy(result, em.wireLog)
	return result
}

func (em *EventManager) GetCmdLog() []node.Event {
	em.mu.RLock()
	defer em.mu.RUnlock()
	result := make([]node.Event, len(em.cmdLog))
	copy(result, em.cmdLog)
	return result
}

func (em *EventManager) GetEventChannel() <-chan node.Event {
	return em.eventCh
}

func (em *EventManager) Close() {
	close(em.eventCh)
}

// ============================================================================
// Command System
// ============================================================================

type CommandHandler struct {
	nodeMgr  *NodeManager
	eventMgr *EventManager
}

func NewCommandHandler(nodeMgr *NodeManager, eventMgr *EventManager) *CommandHandler {
	return &CommandHandler{
		nodeMgr:  nodeMgr,
		eventMgr: eventMgr,
	}
}

func (ch *CommandHandler) HandleCommand(line string) bool {
	if line == "" {
		return false
	}

	parts := strings.Fields(line)
	cmd := strings.ToLower(parts[0])

	emit := func(s string) {
		ch.eventMgr.AddEvent(node.Event{
			Time:   time.Now(),
			Node:   "UI",
			Type:   node.EventType("cmd"),
			Fields: map[string]any{"cmd": s},
		})
	}

	switch cmd {
	case "q", "quit", "exit":
		return true
	case "help", "h", "?":
		ch.showHelp(emit)
	case "put":
		return ch.handlePut(parts, emit)
	case "del":
		return ch.handleDelete(parts, emit)
	case "rand":
		return ch.handleRandom(parts, emit)
	case "link":
		return ch.handleLink(parts, emit)
	case "pause":
		return ch.handlePause(parts, emit)
	case "burst":
		return ch.handleBurst(parts, emit)
	case "net":
		return ch.handleNetwork(parts, emit)
	case "add":
		return ch.handleAddNode(parts, emit)
	case "remove":
		return ch.handleRemoveNode(parts, emit)
	case "stats":
		ch.showStats(emit)
	case "clear":
		ch.eventMgr.mu.Lock()
		ch.eventMgr.events = ch.eventMgr.events[:0]
		ch.eventMgr.wireLog = ch.eventMgr.wireLog[:0]
		ch.eventMgr.cmdLog = ch.eventMgr.cmdLog[:0]
		ch.eventMgr.mu.Unlock()
		emit("events cleared")
	case "cleanup":
		ch.nodeMgr.StopNodes()
		emit("All nodes stopped and ports released.")
	case "dump":
		return ch.handleDump(parts, emit)
	default:
		emit(fmt.Sprintf("unknown command: %s (type 'help' for available commands)", line))
	}
	return false
}

func (ch *CommandHandler) showHelp(emit func(string)) {
	help := `Available commands:
  put <node> <key> <value>    - Put a key-value pair on specified node (0,1,2...)
  del <node> <key>           - Delete a key from specified node (0,1,2...)
  rand [node]                - Put random key-value on random or specified node
  link <node> <up|down>      - Control node connectivity (0,1,2...)
  pause <node> <on|off>      - Pause/unpause a node (0,1,2...)
  burst <node> <count>       - Put multiple random key-values (0,1,2...)
  net <node> <tcp|udp|both> <param> [value] - Configure network chaos
  add                        - Add a new node to the ring
  remove <node>              - Remove a node from the ring (0,1,2...)
  stats                      - Show current statistics
  clear                      - Clear event logs
  cleanup                    - Stop all nodes and release ports
  help                       - Show this help
  quit                       - Exit the application`
	emit(help)
}

func (ch *CommandHandler) handleAddNode(parts []string, emit func(string)) bool {
	if err := ch.nodeMgr.AddNode(); err != nil {
		emit(fmt.Sprintf("failed to add node: %v", err))
		return false
	}
	emit("node added successfully")
	return false
}

func (ch *CommandHandler) handleRemoveNode(parts []string, emit func(string)) bool {
	if len(parts) < 2 {
		emit("usage: remove <node>")
		return false
	}

	nodeIndex, err := ch.parseNodeIndex(parts[1])
	if err != nil {
		emit(err.Error())
		return false
	}

	if err := ch.nodeMgr.RemoveNode(nodeIndex); err != nil {
		emit(fmt.Sprintf("failed to remove node: %v", err))
		return false
	}
	emit(fmt.Sprintf("node %d removed successfully", nodeIndex))
	return false
}

func (ch *CommandHandler) handlePut(parts []string, emit func(string)) bool {
	if len(parts) < 4 {
		emit("usage: put <node> <key> <value>")
		return false
	}

	nodeIndex, err := ch.parseNodeIndex(parts[1])
	if err != nil {
		emit(err.Error())
		return false
	}

	node := ch.nodeMgr.GetNode(nodeIndex)
	if node == nil {
		emit(fmt.Sprintf("node %d not found", nodeIndex))
		return false
	}

	key := parts[2]
	value := strings.Join(parts[3:], " ")
	node.Node.Put(key, []byte(value), node.Actor)
	emit(fmt.Sprintf("put %s=%s on %s", key, value, node.Name))
	return false
}

func (ch *CommandHandler) handleDelete(parts []string, emit func(string)) bool {
	if len(parts) < 3 {
		emit("usage: del <node> <key>")
		return false
	}

	nodeIndex, err := ch.parseNodeIndex(parts[1])
	if err != nil {
		emit(err.Error())
		return false
	}

	node := ch.nodeMgr.GetNode(nodeIndex)
	if node == nil {
		emit(fmt.Sprintf("node %d not found", nodeIndex))
		return false
	}

	key := parts[2]
	node.Node.Delete(key, node.Actor)
	emit(fmt.Sprintf("deleted %s from %s", key, node.Name))
	return false
}

func (ch *CommandHandler) handleRandom(parts []string, emit func(string)) bool {
	nodes := ch.nodeMgr.GetNodes()
	if len(nodes) == 0 {
		emit("no nodes available")
		return false
	}

	var targetNode *SimNode
	if len(parts) >= 2 {
		nodeIndex, err := ch.parseNodeIndex(parts[1])
		if err != nil {
			emit(err.Error())
			return false
		}
		targetNode = ch.nodeMgr.GetNode(nodeIndex)
		if targetNode == nil {
			emit(fmt.Sprintf("node %d not found", nodeIndex))
			return false
		}
	} else {
		targetNode = nodes[rand.Intn(len(nodes))]
	}

	k := generateRandomKey()
	v := fmt.Sprintf("v%d", time.Now().UnixNano()%1000)
	targetNode.Node.Put(k, []byte(v), targetNode.Actor)
	emit(fmt.Sprintf("put random %s=%s on %s", k, v, targetNode.Name))
	return false
}

func (ch *CommandHandler) handleLink(parts []string, emit func(string)) bool {
	if len(parts) < 3 {
		emit("usage: link <node> <up|down>")
		return false
	}

	nodeIndex, err := ch.parseNodeIndex(parts[1])
	if err != nil {
		emit(err.Error())
		return false
	}

	node := ch.nodeMgr.GetNode(nodeIndex)
	if node == nil {
		emit(fmt.Sprintf("node %d not found", nodeIndex))
		return false
	}

	up := parts[2] == "up"
	node.Node.SetConnected(up)
	status := "up"
	if !up {
		status = "down"
	}
	emit(fmt.Sprintf("set %s link %s", node.Name, status))
	return false
}

func (ch *CommandHandler) handlePause(parts []string, emit func(string)) bool {
	if len(parts) < 3 {
		emit("usage: pause <node> <on|off>")
		return false
	}

	nodeIndex, err := ch.parseNodeIndex(parts[1])
	if err != nil {
		emit(err.Error())
		return false
	}

	node := ch.nodeMgr.GetNode(nodeIndex)
	if node == nil {
		emit(fmt.Sprintf("node %d not found", nodeIndex))
		return false
	}

	on := parts[2] == "on"
	node.Node.SetPaused(on)
	status := "paused"
	if !on {
		status = "resumed"
	}
	emit(fmt.Sprintf("%s %s", node.Name, status))
	return false
}

func (ch *CommandHandler) handleBurst(parts []string, emit func(string)) bool {
	if len(parts) < 3 {
		emit("usage: burst <node> <count>")
		return false
	}

	nodeIndex, err := ch.parseNodeIndex(parts[1])
	if err != nil {
		emit(err.Error())
		return false
	}

	node := ch.nodeMgr.GetNode(nodeIndex)
	if node == nil {
		emit(fmt.Sprintf("node %d not found", nodeIndex))
		return false
	}

	count, err := strconv.Atoi(parts[2])
	if err != nil || count <= 0 {
		emit("count must be a positive integer")
		return false
	}

	for i := 0; i < count; i++ {
		k := generateRandomKey()
		v := fmt.Sprintf("v%d", time.Now().UnixNano()%(1000+int64(i)))
		node.Node.Put(k, []byte(v), node.Actor)
	}
	emit(fmt.Sprintf("put %d random key-values on %s", count, node.Name))
	return false
}

func (ch *CommandHandler) handleNetwork(parts []string, emit func(string)) bool {
	if len(parts) < 4 {
		emit("usage: net <node> <tcp|udp|both> <param> [value]")
		return false
	}

	nodeIndex, err := ch.parseNodeIndex(parts[1])
	if err != nil {
		emit(err.Error())
		return false
	}

	node := ch.nodeMgr.GetNode(nodeIndex)
	if node == nil {
		emit(fmt.Sprintf("node %d not found", nodeIndex))
		return false
	}

	path := strings.ToLower(parts[2])
	param := strings.ToLower(parts[3])

	var targets []*transport.ChaosEP
	switch path {
	case "tcp":
		targets = []*transport.ChaosEP{node.TCP}
	case "udp":
		targets = []*transport.ChaosEP{node.UDP}
	case "both":
		targets = []*transport.ChaosEP{node.TCP, node.UDP}
	default:
		emit("path must be tcp|udp|both")
		return false
	}

	return ch.applyNetworkParam(targets, param, parts[4:], emit)
}

func (ch *CommandHandler) applyNetworkParam(targets []*transport.ChaosEP, param string, args []string, emit func(string)) bool {
	setAll := func(fn func(*transport.ChaosEP)) {
		for _, t := range targets {
			fn(t)
		}
	}

	switch param {
	case "up":
		setAll(func(t *transport.ChaosEP) { t.SetUp(true) })
		emit("network link up")
	case "down":
		setAll(func(t *transport.ChaosEP) { t.SetUp(false) })
		emit("network link down")
	case "loss":
		if len(args) < 1 {
			emit("usage: net <n> <p> loss <0..1>")
			return false
		}
		p, err := strconv.ParseFloat(args[0], 64)
		if err != nil || p < 0 || p > 1 {
			emit("loss must be between 0 and 1")
			return false
		}
		setAll(func(t *transport.ChaosEP) { t.SetLoss(p) })
		emit(fmt.Sprintf("loss=%.2f", p))
	case "dup":
		if len(args) < 1 {
			emit("usage: net <n> <p> dup <0..1>")
			return false
		}
		p, err := strconv.ParseFloat(args[0], 64)
		if err != nil || p < 0 || p > 1 {
			emit("dup must be between 0 and 1")
			return false
		}
		setAll(func(t *transport.ChaosEP) { t.SetDup(p) })
		emit(fmt.Sprintf("dup=%.2f", p))
	case "reorder":
		if len(args) < 1 {
			emit("usage: net <n> <p> reorder <0..1>")
			return false
		}
		p, err := strconv.ParseFloat(args[0], 64)
		if err != nil || p < 0 || p > 1 {
			emit("reorder must be between 0 and 1")
			return false
		}
		setAll(func(t *transport.ChaosEP) { t.SetReorder(p) })
		emit(fmt.Sprintf("reorder=%.2f", p))
	case "delay":
		if len(args) < 1 {
			emit("usage: net <n> <p> delay <base> [jitter]")
			return false
		}
		base, err := time.ParseDuration(args[0])
		if err != nil {
			emit("bad duration (e.g. 80ms)")
			return false
		}
		jit := time.Duration(0)
		if len(args) >= 2 {
			jit, err = time.ParseDuration(args[1])
			if err != nil {
				emit("bad jitter duration (e.g. 20ms)")
				return false
			}
		}
		setAll(func(t *transport.ChaosEP) {
			t.SetBaseDelay(base)
			t.SetJitter(jit)
		})
		emit(fmt.Sprintf("delay base=%s jitter=%s", base, jit))
	default:
		emit("unknown network parameter")
		return false
	}
	return false
}

func (ch *CommandHandler) showStats(emit func(string)) {
	nodes := ch.nodeMgr.GetNodes()
	stats := fmt.Sprintf("Nodes: %d | ", len(nodes))

	for i, node := range nodes {
		if i > 0 {
			stats += " | "
		}
		status := "healthy"
		if node.Node.IsPaused() {
			status = "paused"
		} else if !node.Node.IsConnected() {
			status = "disconnected"
		}
		stats += fmt.Sprintf("%s:%s", node.Name, status)
	}

	emit(stats)
}

func (ch *CommandHandler) parseNodeIndex(s string) (int, error) {
	// Support both numeric and letter-based node selection
	if s == "a" || s == "0" {
		return 0, nil
	}
	if s == "b" || s == "1" {
		return 1, nil
	}
	if s == "c" || s == "2" {
		return 2, nil
	}
	if s == "d" || s == "3" {
		return 3, nil
	}
	if s == "e" || s == "4" {
		return 4, nil
	}
	if s == "f" || s == "5" {
		return 5, nil
	}
	if s == "g" || s == "6" {
		return 6, nil
	}
	if s == "h" || s == "7" {
		return 7, nil
	}
	if s == "i" || s == "8" {
		return 8, nil
	}
	if s == "j" || s == "9" {
		return 9, nil
	}

	// Try parsing as number
	if i, err := strconv.Atoi(s); err == nil {
		return i, nil
	}

	return 0, fmt.Errorf("invalid node identifier: %s (use 0-9 or a-j)", s)
}

// handleDump writes leaf hashes for node <idx> or all nodes
func (ch *CommandHandler) handleDump(parts []string, emit func(string)) bool {
	nodes := ch.nodeMgr.GetNodes()
	which := -1
	if len(parts) >= 2 {
		if idx, err := ch.parseNodeIndex(parts[1]); err == nil {
			which = idx
		}
	}
	ts := time.Now().Format("20060102-150405")
	count := 0
	for i, s := range nodes {
		if which >= 0 && i != which {
			continue
		}
		view := materialize.Snapshot(s.Node.Log)
		leaves := materialize.LeavesFromSnapshot(view)
		out := make([]map[string]string, 0, len(leaves))
		for k := 0; k < len(leaves) && k < 2000; k++ {
			out = append(out, map[string]string{"key": leaves[k].Key, "hash": shortHex(leaves[k].Hash[:], 8)})
		}
		fname := fmt.Sprintf("logs/leafdump-%s-node%s.json", ts, s.Name)
		if err := writeJSONFile(fname, out); err != nil {
			emit(fmt.Sprintf("dump error for %s: %v", s.Name, err))
			continue
		}
		count++
	}
	if count == 0 {
		emit("no nodes to dump")
	} else {
		emit(fmt.Sprintf("dumped %d node(s)", count))
	}
	return false
}

// ============================================================================
// Writer and Network Simulation
// ============================================================================

// writerLoop generates random writes to a node at specified intervals
func writerLoop(ctx context.Context, s *SimNode, mean time.Duration) {
	lambda := 1.0 / mean.Seconds()
	for {
		// exponential backoff for Poisson process
		sleep := time.Duration(rand.ExpFloat64()/lambda*1e9) * time.Nanosecond
		select {
		case <-ctx.Done():
			return
		case <-time.After(sleep):
			k := generateRandomKey()
			v := fmt.Sprintf("v%d", rand.Intn(1000))
			s.Node.Put(k, []byte(v), s.Actor)
		}
	}
}

// flapLoop periodically drops one ring link (i <-> i+1) then restores it.
func flapLoop(ctx context.Context, nodes []*SimNode, meanPeriod, down time.Duration) {
	if meanPeriod <= 0 || down <= 0 {
		return
	}
	lambda := 1.0 / meanPeriod.Seconds()
	n := len(nodes)
	for {
		// next failure time ~ Exp(meanPeriod)
		sleep := time.Duration(rand.ExpFloat64()/lambda*1e9) * time.Nanosecond
		select {
		case <-ctx.Done():
			return
		case <-time.After(sleep):
			i := rand.Intn(n)
			j := (i + 1) % n
			// simulate link down between i and j
			nodes[i].Node.SetConnected(false)
			nodes[j].Node.SetConnected(false)
			time.Sleep(down)
			nodes[i].Node.SetConnected(true)
			nodes[j].Node.SetConnected(true)
		}
	}
}

// ============================================================================
// Bubble Tea TUI Model
// ============================================================================

type tuiModel struct {
	nodeMgr    *NodeManager
	eventMgr   *EventManager
	cmdHandler *CommandHandler
	logs       *LogsManager

	// UI components
	nodesTable table.Model
	eventsView viewport.Model
	wireView   viewport.Model
	cmdView    viewport.Model
	input      string

	// State
	width  int
	height int

	// Panel visibility
	showWire   bool
	showEvents bool
	showSV     bool
	showCmds   bool
}

func initialModel(nodeMgr *NodeManager, eventMgr *EventManager, cmdHandler *CommandHandler, logs *LogsManager) tuiModel {
	// Initialize table
	columns := []table.Column{
		{Title: "ID", Width: 3},
		{Title: "Status", Width: 10},
		{Title: "Link", Width: 6},
		{Title: "Paused", Width: 6},
		{Title: "Keys", Width: 6},
		{Title: "Root", Width: 12},
		{Title: "Port", Width: 6},
	}

	t := table.New(
		table.WithColumns(columns),
		table.WithFocused(true),
		table.WithHeight(5), // Start with a reasonable height
	)

	// Style the table
	s := table.DefaultStyles()
	s.Header = s.Header.
		BorderStyle(lipgloss.RoundedBorder()).
		BorderForeground(lipgloss.Color("240")).
		BorderBottom(true).
		Bold(true)
	s.Selected = s.Selected.
		Foreground(lipgloss.Color("229")).
		Background(lipgloss.Color("57")).
		Bold(false)
	t.SetStyles(s)

	// Initialize viewports without inner borders (outer panels will provide borders)
	eventsVP := viewport.New(80, 10)
	eventsVP.Style = lipgloss.NewStyle()

	wireVP := viewport.New(80, 10)
	wireVP.Style = lipgloss.NewStyle()

	cmdVP := viewport.New(80, 10)
	cmdVP.Style = lipgloss.NewStyle()

	return tuiModel{
		nodeMgr:    nodeMgr,
		eventMgr:   eventMgr,
		cmdHandler: cmdHandler,
		logs:       logs,
		nodesTable: t,
		eventsView: eventsVP,
		wireView:   wireVP,
		cmdView:    cmdVP,
		input:      "",
		width:      120, // Default width
		height:     30,  // Default height
		showWire:   true,
		showEvents: true,
		showSV:     true,
		showCmds:   true,
	}
}

func (m tuiModel) Init() tea.Cmd {
	return tea.Batch(
		tea.EnterAltScreen,
		tea.SetWindowTitle("MAEP Sync TUI"),
		tea.Tick(time.Millisecond*100, func(t time.Time) tea.Msg {
			return tickMsg(t)
		}),
	)
}

// tickMsg is a message sent on a timer
type tickMsg time.Time

func (m tuiModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	var cmd tea.Cmd

	switch msg := msg.(type) {
	case tea.KeyMsg:
		switch msg.String() {
		case "ctrl+c", "q":
			return m, tea.Quit
		case "enter":
			if m.input != "" {
				before := takeSnapshot(m.nodeMgr)
				quit := m.cmdHandler.HandleCommand(m.input)
				after := takeSnapshot(m.nodeMgr)
				if m.logs != nil {
					m.logs.LogCommand(m.input, before, after, time.Now())
				}
				if quit {
					return m, tea.Quit
				}
				m.input = ""
			}
		case "backspace":
			if len(m.input) > 0 {
				m.input = m.input[:len(m.input)-1]
			}
		case "ctrl+w":
			m.showWire = !m.showWire
			m.updateLayout()
		case "ctrl+e":
			m.showEvents = !m.showEvents
			m.updateLayout()
		case "ctrl+v":
			m.showSV = !m.showSV
			m.updateLayout()
		case "ctrl+h":
			m.showCmds = !m.showCmds
			m.updateLayout()
		default:
			if len(msg.String()) == 1 && msg.String()[0] >= 32 && msg.String()[0] <= 126 {
				m.input += msg.String()
			}
		}
	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height
		m.updateLayout()
	case tickMsg:
		m.updateData()
		return m, tea.Tick(time.Millisecond*100, func(t time.Time) tea.Msg {
			return tickMsg(t)
		})
	}

	// Update table
	m.nodesTable, cmd = m.nodesTable.Update(msg)

	return m, cmd
}

func (m tuiModel) View() string {
	// Header
	header := lipgloss.NewStyle().
		Bold(true).
		Foreground(lipgloss.Color("63")).
		Render("MAEP Sync TUI") +
		lipgloss.NewStyle().
			Foreground(lipgloss.Color("240")).
			Render(fmt.Sprintf(" — %s", time.Now().Format("15:04:05")))

	// Status line
	nodes := m.nodeMgr.GetNodes()
	status := "IN SYNC"
	statusColor := lipgloss.Color("10")

	if len(nodes) >= 2 {
		// Check if all nodes are in sync
		roots := make([][32]byte, len(nodes))
		for i, node := range nodes {
			view := materialize.Snapshot(node.Node.Log)
			leaves := materialize.LeavesFromSnapshot(view)
			roots[i] = merkle.Build(leaves)
		}

		allInSync := true
		for i := 1; i < len(roots); i++ {
			if roots[i] != roots[0] {
				allInSync = false
				break
			}
		}

		if !allInSync {
			status = "OUT OF SYNC"
			statusColor = lipgloss.Color("9")
		}
	}

	statusLine := lipgloss.NewStyle().
		Foreground(statusColor).
		Bold(true).
		Render(status) +
		lipgloss.NewStyle().
			Foreground(lipgloss.Color("240")).
			Render(fmt.Sprintf(" | Nodes: %d", len(nodes)))
	// Add quick hint for toggles
	toggles := lipgloss.NewStyle().Foreground(lipgloss.Color("240")).Render(" | Toggle panels: [w]ire [e]vents [v]sv [c]mds")
	statusLine += toggles

	// Update table data
	m.updateTableData()

	// Calculate available space
	availableHeight := m.height - 8                     // Reserve space for header, status, input, etc.
	tableHeight := min(len(nodes)+2, availableHeight/2) // Table takes half the available space
	if tableHeight < 3 {
		tableHeight = 3
	}
	viewportHeight := max(5, availableHeight-tableHeight-2) // Remaining space for viewports

	// Update table height
	m.nodesTable.SetHeight(tableHeight)

	// Prepare panel style with minimal padding
	panelStyle := lipgloss.NewStyle().
		Border(lipgloss.RoundedBorder()).
		BorderForeground(lipgloss.Color("62"))
	// No padding to maximize content area
	// .Padding(0, 0) is default; ensure no padding applied

	availableWidth := m.width - 4 // Account for some margin
	if availableWidth < 60 {
		availableWidth = 60
	}
	// Dynamic panel widths based on visibility
	type panelInfo struct {
		visible bool
		weight  int
		min     int
		w       *int
	}
	wireW, eventsW, svW, cmdW := 0, 0, 0, 0
	panels := []panelInfo{
		{m.showWire, 1, 14, &wireW},
		{m.showEvents, 2, 24, &eventsW},
		{m.showSV, 1, 16, &svW},
		{m.showCmds, 1, 24, &cmdW},
	}
	totalWeight := 0
	visibleCount := 0
	for _, p := range panels {
		if p.visible {
			totalWeight += p.weight
			visibleCount++
		}
	}
	if visibleCount == 0 {
		// Nothing visible, show a small note and return
		return header + "\n" + statusLine + "\n\n" + lipgloss.NewStyle().Foreground(lipgloss.Color("240")).Render("No panels visible. Press w/e/v/c to toggle panels.")
	}
	// First pass by weights
	remaining := availableWidth
	for i := range panels {
		if !panels[i].visible {
			continue
		}
		alloc := availableWidth * panels[i].weight / totalWeight
		if alloc < panels[i].min {
			alloc = panels[i].min
		}
		*panels[i].w = alloc
		remaining -= alloc
	}
	// If we overflowed, remaining will be negative; reduce in order: events, cmds, wire, sv
	if remaining < 0 {
		reduceOrder := []*panelInfo{&panels[1], &panels[3], &panels[0], &panels[2]}
		overflow := -remaining
		for _, p := range reduceOrder {
			if !(*p).visible || *(*p).w <= (*p).min {
				continue
			}
			canReduce := *(*p).w - (*p).min
			delta := canReduce
			if delta > overflow {
				delta = overflow
			}
			*(*p).w -= delta
			overflow -= delta
			if overflow == 0 {
				break
			}
		}
	}

	// Calculate inner content widths subtracting frame (border only)
	hFrame, _ := panelStyle.GetFrameSize()
	wireContentW := max(0, wireW-hFrame)
	eventsContentW := max(0, eventsW-hFrame)
	svContentW := max(0, svW-hFrame)
	cmdContentW := max(0, cmdW-hFrame)

	// Size viewports (events and wire existing, add commands viewport)
	m.eventsView.Width = eventsContentW
	m.eventsView.Height = viewportHeight
	m.wireView.Width = wireContentW
	m.wireView.Height = viewportHeight
	m.cmdView.Width = cmdContentW
	m.cmdView.Height = viewportHeight

	// Build panels
	var panelsRendered []string
	if m.showWire {
		wirePanel := panelStyle.
			Width(wireW).
			Height(viewportHeight + 1).
			Render(lipgloss.NewStyle().Bold(true).Render("Wire") + "\n" + m.wireView.View())
		panelsRendered = append(panelsRendered, wirePanel)
	}

	// Events content already in eventsView
	if m.showEvents {
		eventsPanel := panelStyle.
			Width(eventsW).
			Height(viewportHeight + 1).
			Render(lipgloss.NewStyle().Bold(true).Render("Events") + "\n" + m.eventsView.View())
		panelsRendered = append(panelsRendered, eventsPanel)
	}

	// SV panel
	if m.showSV {
		var svb strings.Builder
		svb.WriteString("Node SV (segs/keys top):\n")
		for _, n := range nodes {
			sv := m.eventMgr.svByNode[n.Name]
			top := strings.Join(sv.Top, ",")
			svb.WriteString(fmt.Sprintf("%s: %d/%d [%s]\n", n.Name, sv.Segs, sv.Keys, top))
		}
		svPanel := panelStyle.
			Width(svW).
			Height(viewportHeight + 1).
			Render(lipgloss.NewStyle().Bold(true).Render("SV") + "\n" + lipgloss.NewStyle().Width(svContentW).Render(svb.String()))
		panelsRendered = append(panelsRendered, svPanel)
	}

	// Commands: render from cmd log into cmdView
	if m.showCmds {
		cmds := m.eventMgr.GetCmdLog()
		var cb strings.Builder
		for i := len(cmds) - 1; i >= 0 && i >= len(cmds)-200; i-- {
			cb.WriteString(formatEvent(cmds[i]))
			cb.WriteByte('\n')
		}
		m.cmdView.SetContent(cb.String())
		cmdPanel := panelStyle.
			Width(cmdW).
			Height(viewportHeight + 1).
			Render(lipgloss.NewStyle().Bold(true).Render("Commands") + "\n" + m.cmdView.View())
		panelsRendered = append(panelsRendered, cmdPanel)
	}

	doc := strings.Builder{}
	doc.WriteString(header)
	doc.WriteString("\n")
	doc.WriteString(statusLine)
	doc.WriteString("\n\n")

	// Nodes table
	doc.WriteString(lipgloss.NewStyle().Bold(true).Render("Nodes:"))
	doc.WriteString("\n")
	doc.WriteString(m.nodesTable.View())
	doc.WriteString("\n\n")

	// Panels: title + viewport content inside bordered boxes
	doc.WriteString(lipgloss.JoinHorizontal(lipgloss.Top, panelsRendered...))
	doc.WriteString("\n\n")

	// Input line
	inputLine := lipgloss.NewStyle().
		Foreground(lipgloss.Color("63")).
		Bold(true).
		Render("> ") + m.input + "_"
	doc.WriteString(inputLine)

	return doc.String()
}

func (m *tuiModel) updateLayout() {
	// Adjust table height based on available space and number of nodes
	nodes := m.nodeMgr.GetNodes()
	availableHeight := m.height - 8 // Reserve space for header, status, input, etc.

	// Table should show all nodes plus header
	tableHeight := min(len(nodes)+2, availableHeight/2)
	if tableHeight < 3 {
		tableHeight = 3 // Minimum height for table
	}

	m.nodesTable.SetHeight(tableHeight)

	// Update viewport heights
	viewportHeight := max(5, availableHeight-tableHeight-2)
	m.eventsView.Height = viewportHeight
	m.wireView.Height = viewportHeight
	m.cmdView.Height = viewportHeight
}

func (m *tuiModel) updateData() {
	// Update table data
	m.updateTableData()

	// Drain any pending events into buffers
	for {
		select {
		case e := <-m.eventMgr.GetEventChannel():
			m.eventMgr.AddEvent(e)
		default:
			goto drained
		}
	}

drained:
	// Update viewport content (no headers here; titles are rendered in panels)
	events := m.eventMgr.GetEvents()
	wireLog := m.eventMgr.GetWireLog()

	var b strings.Builder
	for i := len(events) - 1; i >= 0 && i >= len(events)-200; i-- {
		b.WriteString(formatEvent(events[i]))
		b.WriteByte('\n')
	}
	m.eventsView.SetContent(b.String())

	b.Reset()
	for i := len(wireLog) - 1; i >= 0 && i >= len(wireLog)-200; i-- {
		b.WriteString(formatWireEvent(wireLog[i]))
		b.WriteByte('\n')
	}
	m.wireView.SetContent(b.String())

	// Update command view
	cmds := m.eventMgr.GetCmdLog()
	var cb strings.Builder
	for i := len(cmds) - 1; i >= 0 && i >= len(cmds)-200; i-- {
		cb.WriteString(formatEvent(cmds[i]))
		cb.WriteByte('\n')
	}
	m.cmdView.SetContent(cb.String())
}

func (m *tuiModel) updateTableData() {
	nodes := m.nodeMgr.GetNodes()
	rows := make([]table.Row, len(nodes))

	for i, node := range nodes {
		// Get node status
		status := "healthy"
		if node.Node.IsPaused() {
			status = "paused"
		} else if !node.Node.IsConnected() {
			status = "disconnected"
		}

		// Get node data
		view := materialize.Snapshot(node.Node.Log)
		leaves := materialize.LeavesFromSnapshot(view)
		root := merkle.Build(leaves)

		// Count keys
		keyCount := 0
		for _, state := range view {
			if state.Present {
				keyCount++
			}
		}

		rows[i] = table.Row{
			node.Name,
			status,
			ifThen(node.Node.IsConnected(), "up", "down"),
			ifThen(node.Node.IsPaused(), "yes", "no"),
			fmt.Sprintf("%d", keyCount),
			shortHex(root[:], 8),
			fmt.Sprintf("%d", node.Port),
		}
	}

	m.nodesTable.SetRows(rows)
}

func main() {
	// Parse and validate configuration
	cfg := parseFlags()
	if err := cfg.Validate(); err != nil {
		log.Fatalf("Configuration error: %v", err)
	}
	// Silence node slog to avoid corrupting TUI on bursts
	slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError})))

	// Create managers
	nodeMgr := NewNodeManager(cfg)
	eventMgr := NewEventManager(4096)
	cmdHandler := NewCommandHandler(nodeMgr, eventMgr)

	// Create logs manager
	logs, err := NewLogsManager("logs")
	if err != nil {
		log.Fatalf("Failed to init logs: %v", err)
	}
	defer logs.Close()

	// Create nodes
	if err := nodeMgr.CreateNodes(); err != nil {
		log.Fatalf("Failed to create nodes: %v", err)
	}
	if err := nodeMgr.StartNodes(); err != nil {
		log.Fatalf("Failed to start nodes: %v", err)
	}
	defer nodeMgr.StopNodes()

	// Forward node events to memory and disk
	nodes := nodeMgr.GetNodes()
	for _, sn := range nodes {
		ch := sn.Node.GetEvents()
		go func(ch <-chan node.Event) {
			for e := range ch {
				eventMgr.AddEvent(e)
				if e.Type == node.EventWire {
					logs.LogWire(e.Node, e.Fields, e.Time)
				} else {
					logs.LogEvent(e.Node, string(e.Type), e.Fields, e.Time)
				}
			}
		}(ch)
	}

	// Run TUI
	p := tea.NewProgram(initialModel(nodeMgr, eventMgr, cmdHandler, logs))
	if _, err := p.Run(); err != nil {
		log.Fatalf("TUI error: %v", err)
	}
}
