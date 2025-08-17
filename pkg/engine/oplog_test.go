package engine

import (
	"bytes"
	"testing"
)

func Test_OPLOG_Append_00(t *testing.T) {

	opLog := NewOpLog()
	ops := generateOps(10)
	for _, o := range ops {
		opLog.Append(&o)
	}

	if opLog.Len() != len(ops) {
		t.Fatalf("oplog append fail, expected %d ops, %d received", len(ops), opLog.Len())
	}

}

func Test_OPLOG_AppendIdempotence_01(t *testing.T) {
	opLog := NewOpLog()
	ops := generateOps(10)
	for _, o := range ops {
		opLog.Append(&o)
	}

	for _, o := range ops {
		opLog.Append(&o)
	}

	if opLog.Len() != len(ops) {
		t.Fatalf("oplog append fail, expected %d ops, %d received", len(ops), opLog.Len())
	}

}

func Test_OPLOG_GetOrdered_02(t *testing.T) {
	opLog := NewOpLog()

	// Create 5 operations with known values
	ops := []Op{
		generateOp(
			WithKey([]byte("key1")),
			WithValue([]byte("value1")),
		),
		generateOp(
			WithKey([]byte("key2")),
			WithValue([]byte("value2")),
		),
		generateOp(
			WithKey([]byte("key3")),
			WithValue([]byte("value3")),
		),
		generateOp(
			WithKey([]byte("key4")),
			WithValue([]byte("value4")),
		),
		generateOp(
			WithKey([]byte("key5")),
			WithValue([]byte("value5")),
		),
	}

	// Append operations in reverse order to test that GetOrdered returns them in correct order
	for i := len(ops) - 1; i >= 0; i-- {
		opLog.Append(&ops[i])
	}

	// Verify the log has all operations
	if opLog.Len() != len(ops) {
		t.Fatalf("expected %d operations in log, got %d", len(ops), opLog.Len())
	}

	// Get ordered operations
	orderedOps := make([]*Op, 0)
	orderedKeys := make([]OpCannonicalKey, 0)
	for key, op := range opLog.GetOrdered() {
		orderedOps = append(orderedOps, op)
		orderedKeys = append(orderedKeys, key)
	}

	// Verify we got all operations
	if len(orderedOps) != len(ops) {
		t.Fatalf("expected %d ordered operations, got %d", len(ops), len(orderedOps))
	}

	// Verify operations are ordered by their canonical key
	for i := range len(orderedKeys) - 1 {
		if bytes.Compare(orderedKeys[i][:], orderedKeys[i+1][:]) >= 0 {
			t.Errorf("operations not properly ordered: key %d should come before key %d", i, i+1)
		}
	}

	// Create a map of expected key-value pairs to verify all operations are present
	expectedPairs := map[string]string{
		"key1": "value1",
		"key2": "value2",
		"key3": "value3",
		"key4": "value4",
		"key5": "value5",
	}

	// Verify all expected operations are present in the ordered result
	foundPairs := make(map[string]string)
	for _, op := range orderedOps {
		keyStr := string(op.Key)
		valueStr := string(op.Value)
		foundPairs[keyStr] = valueStr
	}

	// Check that all expected pairs are found
	for expectedKey, expectedValue := range expectedPairs {
		if foundValue, exists := foundPairs[expectedKey]; !exists {
			t.Errorf("expected key %s not found in ordered operations", expectedKey)
		} else if foundValue != expectedValue {
			t.Errorf("for key %s: expected value %s, got %s", expectedKey, expectedValue, foundValue)
		}
	}

	// Check that no unexpected operations are present
	if len(foundPairs) != len(expectedPairs) {
		t.Errorf("expected %d operations, found %d", len(expectedPairs), len(foundPairs))
	}
}

func Test_OPLOG_GET_03(t *testing.T) {
	opLog := NewOpLog()
	ops := generateOps(5)

	for _, o := range ops {
		opLog.Append(&o)
	}

	so := opLog.Get(ops[1].Hash())

	if !bytes.Equal(so.Encode(), ops[1].Encode()) {
		t.Errorf("expected get to return op")
	}
}
