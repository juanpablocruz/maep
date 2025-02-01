package node

import (
	"encoding/json"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	"github.com/juanpablocruz/maepsim/internal/merkle"
	"github.com/juanpablocruz/maepsim/internal/protocol"
)

func TestHandleJoin(t *testing.T) {
	n := New("test-node", "127.0.0.1:0")
	joinReq := struct {
		NodeID  string `json:"node_id"`
		Address string `json:"address"`
	}{
		NodeID:  "joining-node",
		Address: "127.0.0.1:9000",
	}
	payload, err := json.Marshal(joinReq)
	if err != nil {
		t.Fatalf("Failed to marshal join request: %v", err)
	}
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()
	go n.handleJoin(server, payload)
	respHeader, err := protocol.DecodeHeader(client)
	if err != nil {
		t.Fatalf("Failed to decode join response header: %v", err)
	}
	respPayload := make([]byte, respHeader.Length)
	if _, err := io.ReadFull(client, respPayload); err != nil {
		t.Fatalf("Failed to read join response payload: %v", err)
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
		t.Fatalf("Failed to unmarshal join response: %v", err)
	}
	if joinResp.NewPred != n.Address {
		t.Errorf("Expected new_pred %s, got %s", n.Address, joinResp.NewPred)
	}
}

func TestHandleSync(t *testing.T) {
	n := New("test-node", "127.0.0.1:0")
	n.State.AddData("key1", "value1")
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()
	go n.handleSync(server, []byte{})
	respHeader, err := protocol.DecodeHeader(client)
	if err != nil {
		t.Fatalf("Failed to decode sync response header: %v", err)
	}
	respPayload := make([]byte, respHeader.Length)
	if _, err := io.ReadFull(client, respPayload); err != nil {
		t.Fatalf("Failed to read sync response payload: %v", err)
	}
	var state struct {
		RootHash string                     `json:"root_hash"`
		Data     map[string]merkle.DataItem `json:"data"`
	}
	if err := json.Unmarshal(respPayload, &state); err != nil {
		t.Fatalf("Failed to unmarshal sync response: %v", err)
	}
	expected := fmt.Sprintf("%x", n.State.GetRootHash())
	if state.RootHash != expected {
		t.Errorf("Sync state mismatch: got %s, want %s", state.RootHash, expected)
	}
}

func TestDeltaSync(t *testing.T) {
	n := New("test-node", "127.0.0.1:0")
	n.State.AddData("key1", "value1")
	remoteState := map[string]merkle.DataItem{
		"key1": {Key: "key1", Value: "value2", Timestamp: time.Now().Add(1 * time.Second).UnixNano()},
		"key2": {Key: "key2", Value: "value3", Timestamp: time.Now().UnixNano()},
	}
	n.deltaSyncWithSuccessor(remoteState)
	data := n.State.GetData()
	if item, ok := data["key1"]; !ok || item.Value != "value2" {
		t.Errorf("Expected key1 updated to value2, got %+v", item)
	}
	if _, ok := data["key2"]; !ok {
		t.Error("Expected key2 to be present after delta sync")
	}
}

func TestNodeOfflineRejoin(t *testing.T) {
	n := New("test-node", "127.0.0.1:0")
	n.Start()
	time.Sleep(100 * time.Millisecond)
	n.State.AddData("key1", "value1")
	n.Stop()
	n.State.AddData("key2", "value2")
	n.Start()
	time.Sleep(100 * time.Millisecond)
	data := n.State.GetData()
	if _, ok := data["key2"]; !ok {
		t.Error("Expected key2 to be present after rejoin")
	}
}
