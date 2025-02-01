package tui

import "sync"

// NodeStatus holds a summary of a node’s status.
type NodeStatus struct {
	ID      string // e.g. "node-0"
	Address string // e.g. "127.0.0.1:8000"
	Online  bool   // true if the node is up
	Succ    string // Successor address
	Pred    string // Predecessor address
}

var (
	// Global slice of node statuses.
	nodesStatus []NodeStatus
	nodesMu     sync.Mutex

	// LogBuffer holds recent log lines.
	logBuffer []string
	logMu     sync.Mutex
)

// UpdateNodesStatus replaces the whole nodes status slice.
func UpdateNodesStatus(status []NodeStatus) {
	nodesMu.Lock()
	defer nodesMu.Unlock()
	nodesStatus = status
}

// GetNodesStatus returns a copy of the current node statuses.
func GetNodesStatus() []NodeStatus {
	nodesMu.Lock()
	defer nodesMu.Unlock()
	copyStatus := make([]NodeStatus, len(nodesStatus))
	copy(copyStatus, nodesStatus)
	return copyStatus
}

// AppendLog appends a new line to the log buffer.
func AppendLog(line string) {
	logMu.Lock()
	defer logMu.Unlock()
	logBuffer = append(logBuffer, line)
	// Optionally keep only the last N lines.
	if len(logBuffer) > 200 {
		logBuffer = logBuffer[len(logBuffer)-200:]
	}
}

// GetLogBuffer returns a copy of the current log buffer.
func GetLogBuffer() []string {
	logMu.Lock()
	defer logMu.Unlock()
	copyBuffer := make([]string, len(logBuffer))
	copy(copyBuffer, logBuffer)
	return copyBuffer
}
