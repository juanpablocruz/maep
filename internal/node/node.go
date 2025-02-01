package node

import (
	"encoding/json"
	"fmt"
	"io"
	"net"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/juanpablocruz/maepsim/internal/merkle"
	"github.com/juanpablocruz/maepsim/internal/protocol"
	"github.com/juanpablocruz/maepsim/internal/tui"
)

func isTransientError(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "broken pipe") ||
		strings.Contains(msg, "connection reset by peer") ||
		strings.Contains(msg, "connection refused")
}

// Node represents a MAEP agent.
type Node struct {
	ID      string // Unique identifier.
	Address string // "host:port"
	Succ    string // Successor node's address.
	Pred    string // Predecessor node's address.
	State   *merkle.MerkleTree

	listener net.Listener
	mu       sync.Mutex
}

// New creates a new node.
func New(id, address string) *Node {
	return &Node{
		ID:      id,
		Address: address,
		State:   merkle.NewMerkleTree(),
	}
}

// Start begins listening for incoming connections and starts neighbor monitoring.
func (n *Node) Start() {
	n.mu.Lock()
	defer n.mu.Unlock()

	ln, err := net.Listen("tcp", n.Address)
	if err != nil {
		tui.AppendLog(fmt.Sprintf("ERROR: Node %s failed to listen on %s: %v", n.ID, n.Address, err))
		return
	}
	n.listener = ln

	tui.AppendLog(fmt.Sprintf("Node started: %s at %s", n.ID, n.Address))

	go n.acceptLoop()
	go n.monitorNeighbors()
}

// Stop stops the node’s listener.
func (n *Node) Stop() {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.listener != nil {
		n.listener.Close()
		n.listener = nil
		tui.AppendLog(fmt.Sprintf("Node stopped: %s", n.ID))
	}
}

// acceptLoop continuously accepts incoming connections.
func (n *Node) acceptLoop() {
	for {
		conn, err := n.listener.Accept()
		if err != nil {
			// Listener closed.
			return
		}
		go n.handleConnection(conn)
	}
}

// handleConnection dispatches an incoming connection based on the message type.
func (n *Node) handleConnection(conn net.Conn) {
	defer conn.Close()
	header, err := protocol.DecodeHeader(conn)
	if err != nil {
		if err == io.EOF {
			// This is expected if a client connected and immediately closed.
			tui.AppendLog(fmt.Sprintf("INFO: Connection closed by peer at node %s before header was sent", n.ID))
		} else {
			tui.AppendLog(fmt.Sprintf("ERROR: Node %s failed to decode header: %v", n.ID, err))
		}
		return
	}
	payload := make([]byte, header.Length)
	if _, err := io.ReadFull(conn, payload); err != nil {
		tui.AppendLog(fmt.Sprintf("ERROR: Node %s failed to read payload: %v", n.ID, err))
		return
	}
	switch header.Type {
	case protocol.MsgJoin:
		n.handleJoin(conn, payload)
	case protocol.MsgSync:
		n.handleSync(conn, payload)
	case protocol.MsgDeltaSync:
		n.handleDeltaSync(conn, payload)
	case protocol.MsgPing:
		n.handlePing(conn, payload)
	case protocol.MsgID:
		n.handleID(conn, payload)
	case protocol.MsgAckID:
		n.handleAckID(conn, payload)
	default:
		tui.AppendLog(fmt.Sprintf("WARN: Node %s received unknown message type: %d", n.ID, header.Type))
	}
}

// handleJoin processes a JOIN request.
func (n *Node) handleJoin(conn net.Conn, payload []byte) {
	var joinReq struct {
		NodeID  string `json:"node_id"`
		Address string `json:"address"`
	}
	if err := json.Unmarshal(payload, &joinReq); err != nil {
		tui.AppendLog(fmt.Sprintf("ERROR: Node %s received invalid join payload: %v", n.ID, err))
		return
	}
	tui.AppendLog(fmt.Sprintf("Node %s received join request from %s (%s)", n.ID, joinReq.NodeID, joinReq.Address))

	// For simulation, insert the new node immediately after this node.
	oldSucc := n.Succ
	n.Succ = joinReq.Address

	// Prepare join response: new successor (oldSucc), new predecessor (this node), and current state.
	joinResp := struct {
		NewSucc      string `json:"new_succ"`
		NewPred      string `json:"new_pred"`
		CurrentState struct {
			RootHash string                     `json:"root_hash"`
			Data     map[string]merkle.DataItem `json:"data"`
		} `json:"current_state"`
	}{
		NewSucc: oldSucc,
		NewPred: n.Address,
	}
	joinResp.CurrentState.RootHash = fmt.Sprintf("%x", n.State.GetRootHash())
	joinResp.CurrentState.Data = n.State.GetData()

	respPayload, err := json.Marshal(joinResp)
	if err != nil {
		tui.AppendLog(fmt.Sprintf("ERROR: Node %s failed to marshal join response: %v", n.ID, err))
		return
	}
	respHeader := protocol.MessageHeader{
		Version: protocol.Version,
		Type:    protocol.MsgJoin,
		Length:  uint32(len(respPayload)),
	}
	encodedHeader, err := protocol.EncodeHeader(respHeader)
	if err != nil {
		tui.AppendLog(fmt.Sprintf("ERROR: Node %s failed to encode join response header: %v", n.ID, err))
		return
	}
	if _, err := conn.Write(encodedHeader); err != nil {
		tui.AppendLog(fmt.Sprintf("ERROR: Node %s failed to write join response header: %v", n.ID, err))
		return
	}
	if _, err := conn.Write(respPayload); err != nil {
		tui.AppendLog(fmt.Sprintf("ERROR: Node %s failed to write join response payload: %v", n.ID, err))
		return
	}
}

// SendJoin initiates a join request to the given target.
func (n *Node) SendJoin(target string) {
	conn, err := net.Dial("tcp", target)
	if err != nil {
		tui.AppendLog(fmt.Sprintf("ERROR: Node %s failed to connect to join target %s: %v", n.ID, target, err))
		return
	}
	defer conn.Close()
	joinReq := struct {
		NodeID  string `json:"node_id"`
		Address string `json:"address"`
	}{
		NodeID:  n.ID,
		Address: n.Address,
	}
	payload, err := json.Marshal(joinReq)
	if err != nil {
		tui.AppendLog(fmt.Sprintf("ERROR: Node %s failed to marshal join request: %v", n.ID, err))
		return
	}
	header := protocol.MessageHeader{
		Version: protocol.Version,
		Type:    protocol.MsgJoin,
		Length:  uint32(len(payload)),
	}
	encodedHeader, err := protocol.EncodeHeader(header)
	if err != nil {
		tui.AppendLog(fmt.Sprintf("ERROR: Node %s failed to encode join header: %v", n.ID, err))
		return
	}
	if _, err := conn.Write(encodedHeader); err != nil {
		tui.AppendLog(fmt.Sprintf("ERROR: Node %s failed to write join header: %v", n.ID, err))
		return
	}
	if _, err := conn.Write(payload); err != nil {
		tui.AppendLog(fmt.Sprintf("ERROR: Node %s failed to write join payload: %v", n.ID, err))
		return
	}
	// Read join response.
	respHeader, err := protocol.DecodeHeader(conn)
	if err != nil {
		tui.AppendLog(fmt.Sprintf("ERROR: Node %s failed to decode join response header: %v", n.ID, err))
		return
	}
	respPayload := make([]byte, respHeader.Length)
	if _, err := io.ReadFull(conn, respPayload); err != nil {
		tui.AppendLog(fmt.Sprintf("ERROR: Node %s failed to read join response payload: %v", n.ID, err))
		return
	}
	var joinResp struct {
		NewSucc      string `json:"new_succ"`
		NewPred      string `json:"new_pred"`
		CurrentState struct {
			RootHash string                     `json:"root_hash"`
			Data     map[string]merkle.DataItem `json:"data"`
		} `json:"current_state"`
	}
	if err := json.Unmarshal(respPayload, &joinResp); err != nil {
		tui.AppendLog(fmt.Sprintf("ERROR: Node %s failed to unmarshal join response: %v", n.ID, err))
		return
	}
	n.Succ = joinResp.NewSucc
	n.Pred = joinResp.NewPred
	n.mergeState(joinResp.CurrentState.Data)
	tui.AppendLog(fmt.Sprintf("Node %s joined network. Pred: %s, Succ: %s", n.ID, n.Pred, n.Succ))
}

// mergeState merges remote state with local state.
func (n *Node) mergeState(remoteData map[string]merkle.DataItem) {
	for _, remoteItem := range remoteData {
		n.State.UpdateData(remoteItem)
	}
}

// handleSync processes a SYNC request by returning the current state.
func (n *Node) handleSync(conn net.Conn, payload []byte) {
	state := struct {
		RootHash string                     `json:"root_hash"`
		Data     map[string]merkle.DataItem `json:"data"`
	}{
		RootHash: fmt.Sprintf("%x", n.State.GetRootHash()),
		Data:     n.State.GetData(),
	}
	respPayload, err := json.Marshal(state)
	if err != nil {
		tui.AppendLog(fmt.Sprintf("ERROR: Node %s failed to marshal sync state: %v", n.ID, err))
		return
	}
	respHeader := protocol.MessageHeader{
		Version: protocol.Version,
		Type:    protocol.MsgSync,
		Length:  uint32(len(respPayload)),
	}
	encodedHeader, err := protocol.EncodeHeader(respHeader)
	if err != nil {
		tui.AppendLog(fmt.Sprintf("ERROR: Node %s failed to encode sync response header: %v", n.ID, err))
		return
	}
	if _, err := conn.Write(encodedHeader); err != nil {
		tui.AppendLog(fmt.Sprintf("ERROR: Node %s failed to write sync response header: %v", n.ID, err))
		return
	}
	if _, err := conn.Write(respPayload); err != nil {
		tui.AppendLog(fmt.Sprintf("ERROR: Node %s failed to write sync response payload: %v", n.ID, err))
		return
	}
}

// handleDeltaSync processes a DELTA SYNC request. It returns the full state.
func (n *Node) handleDeltaSync(conn net.Conn, payload []byte) {
	state := struct {
		RootHash string                     `json:"root_hash"`
		Data     map[string]merkle.DataItem `json:"data"`
	}{
		RootHash: fmt.Sprintf("%x", n.State.GetRootHash()),
		Data:     n.State.GetData(),
	}
	respPayload, err := json.Marshal(state)
	if err != nil {
		tui.AppendLog(fmt.Sprintf("ERROR: Node %s failed to marshal delta sync response: %v", n.ID, err))
		return
	}
	respHeader := protocol.MessageHeader{
		Version: protocol.Version,
		Type:    protocol.MsgDeltaSync,
		Length:  uint32(len(respPayload)),
	}
	encodedHeader, err := protocol.EncodeHeader(respHeader)
	if err != nil {
		tui.AppendLog(fmt.Sprintf("ERROR: Node %s failed to encode delta sync header: %v", n.ID, err))
		return
	}
	if _, err := conn.Write(encodedHeader); err != nil {
		tui.AppendLog(fmt.Sprintf("ERROR: Node %s failed to write delta sync header: %v", n.ID, err))
		return
	}
	if _, err := conn.Write(respPayload); err != nil {
		tui.AppendLog(fmt.Sprintf("ERROR: Node %s failed to write delta sync payload: %v", n.ID, err))
		return
	}
}

// handlePing processes a PING request by returning a "pong".
func (n *Node) handlePing(conn net.Conn, payload []byte) {
	respPayload := []byte("pong")
	respHeader := protocol.MessageHeader{
		Version: protocol.Version,
		Type:    protocol.MsgPing,
		Length:  uint32(len(respPayload)),
	}
	encodedHeader, err := protocol.EncodeHeader(respHeader)
	if err != nil {
		tui.AppendLog(fmt.Sprintf("ERROR: Node %s failed to encode ping response header: %v", n.ID, err))
		return
	}
	if _, err := conn.Write(encodedHeader); err != nil {
		tui.AppendLog(fmt.Sprintf("ERROR: Node %s failed to write ping response header: %v", n.ID, err))
		return
	}
	if _, err := conn.Write(respPayload); err != nil {
		tui.AppendLog(fmt.Sprintf("ERROR: Node %s failed to write ping response payload: %v", n.ID, err))
		return
	}
}

// handleID processes a cord formation ID message.
func (n *Node) handleID(conn net.Conn, payload []byte) {
	var msg struct {
		CordID string `json:"cord_id"`
	}
	if err := json.Unmarshal(payload, &msg); err != nil {
		tui.AppendLog(fmt.Sprintf("ERROR: Node %s received invalid ID message payload: %v", n.ID, err))
		return
	}
	tui.AppendLog(fmt.Sprintf("Node %s received cord formation ID: %s", n.ID, msg.CordID))
	// Send ACK.
	ackMsg := struct {
		CordID string `json:"cord_id"`
	}{
		CordID: msg.CordID,
	}
	ackPayload, err := json.Marshal(ackMsg)
	if err != nil {
		tui.AppendLog(fmt.Sprintf("ERROR: Node %s failed to marshal ACK ID: %v", n.ID, err))
		return
	}
	respHeader := protocol.MessageHeader{
		Version: protocol.Version,
		Type:    protocol.MsgAckID,
		Length:  uint32(len(ackPayload)),
	}
	encodedHeader, err := protocol.EncodeHeader(respHeader)
	if err != nil {
		tui.AppendLog(fmt.Sprintf("ERROR: Node %s failed to encode ACK ID header: %v", n.ID, err))
		return
	}
	if _, err := conn.Write(encodedHeader); err != nil {
		tui.AppendLog(fmt.Sprintf("ERROR: Node %s failed to write ACK ID header: %v", n.ID, err))
		return
	}
	if _, err := conn.Write(ackPayload); err != nil {
		tui.AppendLog(fmt.Sprintf("ERROR: Node %s failed to write ACK ID payload: %v", n.ID, err))
		return
	}
}

// handleAckID processes an ACK ID message.
func (n *Node) handleAckID(conn net.Conn, payload []byte) {
	var msg struct {
		CordID string `json:"cord_id"`
	}
	if err := json.Unmarshal(payload, &msg); err != nil {
		tui.AppendLog(fmt.Sprintf("ERROR: Node %s received invalid ACK ID payload: %v", n.ID, err))
		return
	}
	tui.AppendLog(fmt.Sprintf("Node %s received ACK for cord ID: %s", n.ID, msg.CordID))
}

// SyncWithSuccessor sends a SYNC request to the successor and compares state.
func (n *Node) SyncWithSuccessor() {
	if n.Succ == "" {
		tui.AppendLog(fmt.Sprintf("WARN: Node %s has no successor defined", n.ID))
		return
	}
	conn, err := net.Dial("tcp", n.Succ)
	if err != nil {
		tui.AppendLog(fmt.Sprintf("ERROR: Node %s failed to connect to successor %s: %v", n.ID, n.Succ, err))
		return
	}
	defer conn.Close()

	header := protocol.MessageHeader{
		Version: protocol.Version,
		Type:    protocol.MsgSync,
		Length:  0,
	}
	encodedHeader, err := protocol.EncodeHeader(header)
	if err != nil {
		tui.AppendLog(fmt.Sprintf("ERROR: Node %s failed to encode sync header: %v", n.ID, err))
		return
	}
	if _, err := conn.Write(encodedHeader); err != nil {
		tui.AppendLog(fmt.Sprintf("ERROR: Node %s failed to write sync header: %v", n.ID, err))
		return
	}
	respHeader, err := protocol.DecodeHeader(conn)
	if err != nil {
		tui.AppendLog(fmt.Sprintf("ERROR: Node %s failed to decode sync response header: %v", n.ID, err))
		return
	}
	respPayload := make([]byte, respHeader.Length)
	if _, err := io.ReadFull(conn, respPayload); err != nil {
		tui.AppendLog(fmt.Sprintf("ERROR: Node %s failed to read sync response payload: %v", n.ID, err))
		return
	}
	var state struct {
		RootHash string                     `json:"root_hash"`
		Data     map[string]merkle.DataItem `json:"data"`
	}
	if err := json.Unmarshal(respPayload, &state); err != nil {
		tui.AppendLog(fmt.Sprintf("ERROR: Node %s failed to unmarshal sync response: %v", n.ID, err))
		return
	}
	localRoot := fmt.Sprintf("%x", n.State.GetRootHash())
	if state.RootHash != localRoot {
		tui.AppendLog(fmt.Sprintf("WARN: Node %s state mismatch with successor %s (local: %s, remote: %s)", n.ID, n.Succ, localRoot, state.RootHash))
		n.deltaSyncWithSuccessor(state.Data)
	} else {
		tui.AppendLog(fmt.Sprintf("Node %s state in sync with successor %s", n.ID, n.Succ))
	}
}

// deltaSyncWithSuccessor merges missing data from remote state.
func (n *Node) deltaSyncWithSuccessor(remoteData map[string]merkle.DataItem) {
	updated := false
	localData := n.State.GetData()
	for key, remoteItem := range remoteData {
		localItem, exists := localData[key]
		if !exists || remoteItem.Timestamp > localItem.Timestamp {
			n.State.UpdateData(remoteItem)
			updated = true
		} else if exists && remoteItem.Timestamp == localItem.Timestamp && remoteItem.Value != localItem.Value {
			tui.AppendLog(fmt.Sprintf("WARN: Node %s conflict during delta sync for key %s (local: %s, remote: %s)", n.ID, key, localItem.Value, remoteItem.Value))
		}
	}
	if updated {
		tui.AppendLog(fmt.Sprintf("Node %s delta sync applied", n.ID))
	} else {
		tui.AppendLog(fmt.Sprintf("Node %s delta sync found no differences", n.ID))
	}
}

// monitorNeighbors periodically pings successor and predecessor.
func (n *Node) monitorNeighbors() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if n.Succ != "" && !n.pingNeighbor(n.Succ) {
				tui.AppendLog(fmt.Sprintf("WARN: Node %s successor %s unreachable", n.ID, n.Succ))
				n.initiateReconfiguration()
			}
			if n.Pred != "" && !n.pingNeighbor(n.Pred) {
				tui.AppendLog(fmt.Sprintf("WARN: Node %s predecessor %s unreachable", n.ID, n.Pred))
				n.initiateReconfiguration()
			}
		}
	}
}

// pingNeighbor checks connectivity by attempting a TCP dial and sending a proper ping message.
func (n *Node) pingNeighbor(address string) bool {
	conn, err := net.DialTimeout("tcp", address, 2*time.Second)
	if err != nil {
		return false
	}
	defer conn.Close()

	// Prepare and send ping message.
	msg := "ping"
	header := protocol.MessageHeader{
		Version: protocol.Version,
		Type:    protocol.MsgPing,
		Length:  uint32(len(msg)),
	}
	encodedHeader, err := protocol.EncodeHeader(header)
	if err != nil {
		return false
	}
	if _, err := conn.Write(encodedHeader); err != nil {
		return false
	}
	if _, err := conn.Write([]byte(msg)); err != nil {
		return false
	}

	// Wait for pong reply.
	pongHeader, err := protocol.DecodeHeader(conn)
	if err != nil {
		return false
	}
	pongPayload := make([]byte, pongHeader.Length)
	if _, err := io.ReadFull(conn, pongPayload); err != nil {
		return false
	}
	if string(pongPayload) != "pong" {
		return false
	}
	return true
}

// initiateReconfiguration implements a basic reconfiguration process.
// It uses the global node status (via the tui package) to reassemble a ring from online nodes.
func (n *Node) initiateReconfiguration() {
	tui.AppendLog(fmt.Sprintf("Node %s: Reconfiguration initiated (simulation)", n.ID))

	// Retrieve current global status from the TUI.
	statuses := tui.GetNodesStatus()
	var onlineStatuses []tui.NodeStatus
	for _, ns := range statuses {
		if ns.Online {
			onlineStatuses = append(onlineStatuses, ns)
		}
	}
	if len(onlineStatuses) < 2 {
		tui.AppendLog("WARN: Not enough nodes to reconfigure ring")
		return
	}
	// Sort the online nodes by their ID.
	sort.Slice(onlineStatuses, func(i, j int) bool {
		return onlineStatuses[i].ID < onlineStatuses[j].ID
	})
	// Reassign successor and predecessor pointers in a ring fashion.
	for i := range onlineStatuses {
		onlineStatuses[i].Succ = onlineStatuses[(i+1)%len(onlineStatuses)].Address
		onlineStatuses[i].Pred = onlineStatuses[(i-1+len(onlineStatuses))%len(onlineStatuses)].Address
	}
	// Update the global status.
	tui.UpdateNodesStatus(onlineStatuses)
	tui.AppendLog(fmt.Sprintf("Reconfiguration completed by node %s", n.ID))
}
