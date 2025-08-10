package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/juanpablocruz/maep/pkg/materialize"
	"github.com/juanpablocruz/maep/pkg/merkle"
)

// LogsManager writes JSON lines to event, wire and command files for analysis.
type LogsManager struct {
	mu        sync.Mutex
	baseDir   string
	eventsW   *bufio.Writer
	wireW     *bufio.Writer
	cmdW      *bufio.Writer
	eventsF   *os.File
	wireF     *os.File
	cmdF      *os.File
	startTime time.Time
}

type jsonEvent struct {
	Kind   string         `json:"kind"`
	Time   time.Time      `json:"time"`
	Node   string         `json:"node,omitempty"`
	Type   string         `json:"type,omitempty"`
	Fields map[string]any `json:"fields,omitempty"`
}

type snapshot struct {
	Name      string `json:"name"`
	Port      int    `json:"port"`
	Connected bool   `json:"connected"`
	Paused    bool   `json:"paused"`
	Keys      int    `json:"keys"`
	Root      string `json:"root"`
}

type jsonCommand struct {
	Kind   string     `json:"kind"`
	Time   time.Time  `json:"time"`
	Cmd    string     `json:"cmd"`
	Before []snapshot `json:"before"`
	After  []snapshot `json:"after"`
}

func NewLogsManager(dir string) (*LogsManager, error) {
	base := dir
	if base == "" {
		base = "."
	}
	if err := os.MkdirAll(base, 0o755); err != nil {
		return nil, fmt.Errorf("make log dir: %w", err)
	}
	ts := time.Now().Format("20060102-150405")
	ef, err := os.Create(filepath.Join(base, fmt.Sprintf("events-%s.jsonl", ts)))
	if err != nil {
		return nil, err
	}
	wf, err := os.Create(filepath.Join(base, fmt.Sprintf("wire-%s.jsonl", ts)))
	if err != nil {
		_ = ef.Close()
		return nil, err
	}
	cf, err := os.Create(filepath.Join(base, fmt.Sprintf("commands-%s.jsonl", ts)))
	if err != nil {
		_ = ef.Close()
		_ = wf.Close()
		return nil, err
	}
	return &LogsManager{
		baseDir:   base,
		eventsF:   ef,
		wireF:     wf,
		cmdF:      cf,
		eventsW:   bufio.NewWriterSize(ef, 64<<10),
		wireW:     bufio.NewWriterSize(wf, 64<<10),
		cmdW:      bufio.NewWriterSize(cf, 64<<10),
		startTime: time.Now(),
	}, nil
}

func (lm *LogsManager) Close() {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	if lm.eventsW != nil {
		_ = lm.eventsW.Flush()
	}
	if lm.wireW != nil {
		_ = lm.wireW.Flush()
	}
	if lm.cmdW != nil {
		_ = lm.cmdW.Flush()
	}
	if lm.eventsF != nil {
		_ = lm.eventsF.Close()
	}
	if lm.wireF != nil {
		_ = lm.wireF.Close()
	}
	if lm.cmdF != nil {
		_ = lm.cmdF.Close()
	}
}

func (lm *LogsManager) LogEvent(nodeName, evType string, fields map[string]any, when time.Time) {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	enc := json.NewEncoder(lm.eventsW)
	_ = enc.Encode(jsonEvent{Kind: "event", Time: when, Node: nodeName, Type: evType, Fields: fields})
}

func (lm *LogsManager) LogWire(nodeName string, fields map[string]any, when time.Time) {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	enc := json.NewEncoder(lm.wireW)
	_ = enc.Encode(jsonEvent{Kind: "wire", Time: when, Node: nodeName, Fields: fields})
}

func (lm *LogsManager) LogCommand(cmd string, before, after []snapshot, when time.Time) {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	enc := json.NewEncoder(lm.cmdW)
	_ = enc.Encode(jsonCommand{Kind: "command", Time: when, Cmd: cmd, Before: before, After: after})
}

func takeSnapshot(nm *NodeManager) []snapshot {
	nodes := nm.GetNodes()
	out := make([]snapshot, len(nodes))
	for i, s := range nodes {
		view := materialize.Snapshot(s.Node.Log)
		leaves := materialize.LeavesFromSnapshot(view)
		root := merkle.Build(leaves)
		keys := 0
		for _, st := range view {
			if st.Present {
				keys++
			}
		}
		out[i] = snapshot{
			Name:      s.Name,
			Port:      s.Port,
			Connected: s.Node.IsConnected(),
			Paused:    s.Node.IsPaused(),
			Keys:      keys,
			Root:      shortHex(root[:], 16),
		}
	}
	return out
}
