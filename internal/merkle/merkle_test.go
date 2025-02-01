package merkle_test

import (
	"reflect"
	"strconv"
	"sync"
	"testing"

	"github.com/juanpablocruz/maepsim/internal/merkle"
)

func TestMerkleTree_ComputeRootHashDeterminism(t *testing.T) {
	mt1 := merkle.NewMerkleTree()
	// Use fixed timestamps (for example, 1000) so the result is deterministic.
	keys := []string{"a", "b", "c", "d"}
	for _, k := range keys {
		mt1.AddDataWithTimestamp(k, "val"+k, 1000)
	}
	root1 := mt1.GetRootHash()

	mt2 := merkle.NewMerkleTree()
	for _, k := range keys {
		mt2.AddDataWithTimestamp(k, "val"+k, 1000)
	}
	root2 := mt2.GetRootHash()
	if !reflect.DeepEqual(root1, root2) {
		t.Errorf("Deterministic hash mismatch: %x vs %x", root1, root2)
	}
}

func TestMerkleTree_AddData(t *testing.T) {
	mt := merkle.NewMerkleTree()
	mt.AddDataWithTimestamp("key1", "value1", 1000)
	data := mt.GetData()
	if item, ok := data["key1"]; !ok {
		t.Error("Expected key1 to be present")
	} else if item.Value != "value1" {
		t.Errorf("Expected value1, got %s", item.Value)
	}
	if len(mt.GetRootHash()) == 0 {
		t.Error("Expected non-empty root hash")
	}
}

func TestMerkleTree_ConflictResolution(t *testing.T) {
	mt := merkle.NewMerkleTree()
	mt.AddDataWithTimestamp("key1", "value1", 1000)
	// Use a later timestamp.
	mt.AddDataWithTimestamp("key1", "value2", 2000)
	data := mt.GetData()
	if item, ok := data["key1"]; !ok {
		t.Error("Expected key1 to be present")
	} else if item.Value != "value2" {
		t.Errorf("Expected value2 due to conflict resolution, got %s", item.Value)
	}
}

func TestMerkleTree_ConcurrentAdd(t *testing.T) {
	mt := merkle.NewMerkleTree()
	var wg sync.WaitGroup
	numGoroutines := 50
	keys := []string{"key1", "key2", "key3", "key4", "key5"}
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			key := keys[id%len(keys)]
			mt.AddDataWithTimestamp(key, "value"+strconv.Itoa(id), 1000+int64(id))
		}(i)
	}
	wg.Wait()
	data := mt.GetData()
	if len(data) > len(keys) {
		t.Errorf("Expected at most %d keys, got %d", len(keys), len(data))
	}
	if len(mt.GetRootHash()) == 0 {
		t.Error("Expected non-empty root hash after concurrent adds")
	}
}
