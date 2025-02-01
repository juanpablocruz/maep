package merkle

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/juanpablocruz/maepsim/internal/tui"
)

// DataItem represents a versioned key–value pair.
type DataItem struct {
	Key       string `json:"key"`
	Value     string `json:"value"`
	Timestamp int64  `json:"timestamp"`
}

// MerkleTree tracks state as key–value pairs and computes a SHA‑256 hash over them.
type MerkleTree struct {
	Data     map[string]DataItem
	RootHash []byte
	mu       sync.Mutex
}

// NewMerkleTree creates an empty Merkle tree.
func NewMerkleTree() *MerkleTree {
	return &MerkleTree{
		Data:     make(map[string]DataItem),
		RootHash: []byte{},
	}
}

// AddData inserts or updates a data item using the current time.
func (mt *MerkleTree) AddData(key, value string) {
	mt.AddDataWithTimestamp(key, value, time.Now().UnixNano())
}

// AddDataWithTimestamp inserts or updates a data item using the provided timestamp.
// This method is useful for testing to ensure deterministic hashes.
func (mt *MerkleTree) AddDataWithTimestamp(key, value string, timestamp int64) {
	mt.mu.Lock()
	defer mt.mu.Unlock()
	if existing, ok := mt.Data[key]; ok {
		if timestamp <= existing.Timestamp {
			// If timestamps are equal but values differ, log a conflict.
			if timestamp == existing.Timestamp && existing.Value != value {
				tui.AppendLog(fmt.Sprintf("Conflict detected in AddDataWithTimestamp key=%s existing=%s new=%s", key, existing.Value, value))
			}
			return
		}
	}
	mt.Data[key] = DataItem{
		Key:       key,
		Value:     value,
		Timestamp: timestamp,
	}
	mt.computeRootHash()
}

// UpdateData forces an update using a given DataItem.
func (mt *MerkleTree) UpdateData(item DataItem) {
	mt.mu.Lock()
	defer mt.mu.Unlock()
	current, exists := mt.Data[item.Key]
	if !exists || item.Timestamp > current.Timestamp {
		mt.Data[item.Key] = item
	} else if exists && item.Timestamp == current.Timestamp && item.Value != current.Value {
		tui.AppendLog(fmt.Sprintf("Conflict detected in AddDataWithTimestamp key=%s existing=%s new=%s", item.Key, current.Value, item.Value))
	}
	mt.computeRootHash()
}

// computeRootHash computes a deterministic SHA‑256 hash over the data items in sorted order.
func (mt *MerkleTree) computeRootHash() {
	h := sha256.New()
	// Get sorted keys.
	keys := make([]string, 0, len(mt.Data))
	for k := range mt.Data {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		item := mt.Data[k]
		data, err := json.Marshal(item)
		if err != nil {
			tui.AppendLog(fmt.Sprintf("Failed to marshal data item %s error %v", k, err))
			continue
		}
		h.Write(data)
	}
	mt.RootHash = h.Sum(nil)
}

// GetRootHash returns the current root hash.
func (mt *MerkleTree) GetRootHash() []byte {
	mt.mu.Lock()
	defer mt.mu.Unlock()
	return mt.RootHash
}

// GetData returns a copy of the current data map.
func (mt *MerkleTree) GetData() map[string]DataItem {
	mt.mu.Lock()
	defer mt.mu.Unlock()
	copyMap := make(map[string]DataItem, len(mt.Data))
	for k, v := range mt.Data {
		copyMap[k] = v
	}
	return copyMap
}
