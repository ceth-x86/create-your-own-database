package btree

import (
	"bytes"
	"encoding/binary"
	"testing"
)

// newNode creates a new BNode with the fixed page size.
func newNode() BNode {
	return make([]byte, BTREE_PAGE_SIZE)
}

// expectPanic is a helper function that verifies a function f() panics.
// If f() does not panic, the test fails.
func expectPanic(t *testing.T, f func()) {
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("expected panic but none occurred")
		}
	}()
	f()
}

// TestHeaderOperations verifies the header-related functions.
// It tests that:
// - setHeader correctly sets the node type and number of keys,
// - btype returns the correct node type,
// - nkeys returns the correct key count.
func TestHeaderOperations(t *testing.T) {
	node := newNode()
	node.setHeader(NodeTypeInternal, 5)

	if node.btype() != NodeTypeInternal {
		t.Errorf("expected node type %d, got %d", NodeTypeInternal, node.btype())
	}
	if node.nkeys() != 5 {
		t.Errorf("expected nkeys 5, got %d", node.nkeys())
	}

	node = newNode()
	node.setHeader(NodeTypeLeaf, 3)

	if node.btype() != NodeTypeLeaf {
		t.Errorf("expected node type %d, got %d", NodeTypeLeaf, node.btype())
	}
	if node.nkeys() != 3 {
		t.Errorf("expected nkeys 3, got %d", node.nkeys())
	}
}

// TestPointerOperations verifies pointer-related functions:
// - setPtr writes the pointer value at the given index,
// - getPtr retrieves the correct pointer value,
// - accessing an out-of-bound index results in a panic.
func TestPointerOperations(t *testing.T) {
	node := newNode()
	n := uint16(3)
	node.setHeader(NodeTypeInternal, n)

	// Set pointers with distinct test values.
	for i := uint16(0); i < n; i++ {
		val := uint64(i * 10)
		node.setPtr(i, val)
	}
	// Verify that getPtr returns the correct pointer for each index.
	for i := uint16(0); i < n; i++ {
		expected := uint64(i * 10)
		got := node.getPtr(i)
		if got != expected {
			t.Errorf("at index %d: expected pointer %d, got %d", i, expected, got)
		}
	}

	// Test that accessing an out-of-bound pointer index triggers a panic.
	expectPanic(t, func() {
		_ = node.getPtr(n)
	})
}

// TestOffsetOperations verifies offset-related functions:
//   - getOffset returns 0 for index 0 (start of key-value area),
//   - setOffset correctly writes offsets for valid indices,
//   - invalid offset indices (e.g., index 0 for setting or an index > nkeys)
//     cause a panic.
func TestOffsetOperations(t *testing.T) {
	node := newNode()
	n := uint16(2)
	node.setHeader(NodeTypeLeaf, n)

	// For index 0, getOffset should always return 0.
	if off := node.getOffset(0); off != 0 {
		t.Errorf("expected getOffset(0) = 0, got %d", off)
	}

	// Set offsets for indices 1 and 2.
	node.setOffset(1, 10)
	node.setOffset(2, 20)
	if off := node.getOffset(1); off != 10 {
		t.Errorf("expected getOffset(1) = 10, got %d", off)
	}
	if off := node.getOffset(2); off != 20 {
		t.Errorf("expected getOffset(2) = 20, got %d", off)
	}

	// Setting offset for index 0 should trigger a panic.
	expectPanic(t, func() {
		node.setOffset(0, 5)
	})
	// Accessing an offset with an index greater than nkeys should trigger a panic.
	expectPanic(t, func() {
		_ = node.getOffset(3)
	})
}

// TestKeyValueOperations verifies the key-value related functions:
// - kvPos returns the correct starting position of the key-value record,
// - getKey returns the correct key bytes,
// - getVal returns the correct value bytes,
// - nbytes returns the total number of bytes used in the node.
func TestKeyValueOperations(t *testing.T) {
	node := newNode()
	// Create a leaf node with 1 key-value pair.
	node.setHeader(NodeTypeLeaf, 1)
	// Set offset for the first key-value pair.
	// Record size: 2 bytes (key length) + 2 bytes (value length) + len(key) + len(val).
	// For key = "key1" (4 bytes) and value = "val1" (4 bytes), total = 2+2+4+4 = 12 bytes.
	node.setOffset(1, 12)

	// Get the starting position of the key-value record.
	pos := node.kvPos(0)
	// Write the key length and value length.
	binary.LittleEndian.PutUint16(node[pos:], 4)   // key length = 4
	binary.LittleEndian.PutUint16(node[pos+2:], 4) // value length = 4
	// Write the key and value.
	copy(node[pos+4:], []byte("key1"))
	copy(node[pos+8:], []byte("val1"))

	// Verify that getKey returns the correct key.
	key := node.getKey(0)
	if !bytes.Equal(key, []byte("key1")) {
		t.Errorf("expected key 'key1', got %v", key)
	}
	// Verify that getVal returns the correct value.
	val := node.getVal(0)
	if !bytes.Equal(val, []byte("val1")) {
		t.Errorf("expected value 'val1', got %v", val)
	}

	// Verify that nbytes returns the expected total bytes used in the node.
	expectedNBytes := uint16(4 + 8*1 + 2*1 + 12) // header + pointers + offsets + record = 4+8+2+12 = 26
	if node.nbytes() != expectedNBytes {
		t.Errorf("expected nbytes %d, got %d", expectedNBytes, node.nbytes())
	}
}

// TestNodeKeyValue verifies key-value pair operations:
// - Inserting single key-value pair
// - Retrieving key and value separately
// - Handling multiple key-value pairs
func TestNodeKeyValue(t *testing.T) {
	node := make(BNode, BTREE_PAGE_SIZE)
	node.setHeader(NodeTypeLeaf, 2)

	// Test key-value operations
	key := []byte("test")
	value := []byte("value")
	nodeAppendKV(node, 0, 0, key, value)

	// Verify key
	if got := node.getKey(0); !bytes.Equal(got, key) {
		t.Errorf("Expected key %s, got %s", key, got)
	}

	// Verify value
	if got := node.getVal(0); !bytes.Equal(got, value) {
		t.Errorf("Expected value %s, got %s", value, got)
	}

	// Test multiple key-value pairs
	key2 := []byte("test2")
	value2 := []byte("value2")
	nodeAppendKV(node, 1, 0, key2, value2)

	if got := node.getKey(1); !bytes.Equal(got, key2) {
		t.Errorf("Expected key %s, got %s", key2, got)
	}
	if got := node.getVal(1); !bytes.Equal(got, value2) {
		t.Errorf("Expected value %s, got %s", value2, got)
	}
}

// TestNodeLookupLE verifies the nodeLookupLE function, which searches for the last key that is
// less than or equal to the provided key. This test covers cases with exact matches, values
// between keys, and keys less than the smallest stored key.
func TestNodeLookupLE(t *testing.T) {
	node := newNode()
	n := uint16(3)
	node.setHeader(NodeTypeLeaf, n)
	// Set offsets for three key-value pairs.
	node.setOffset(1, 6)  // first record occupies 6 bytes
	node.setOffset(2, 12) // first two records occupy 12 bytes in total
	node.setOffset(3, 18) // all three records occupy 18 bytes in total

	// Prepare three key-value pairs with keys "a", "c", "e" and values "1", "2", "3".
	keys := [][]byte{[]byte("a"), []byte("c"), []byte("e")}
	vals := [][]byte{[]byte("1"), []byte("2"), []byte("3")}
	for i := uint16(0); i < n; i++ {
		pos := node.kvPos(i)
		// Write key length and value length (both are 1 byte).
		binary.LittleEndian.PutUint16(node[pos:], 1)
		binary.LittleEndian.PutUint16(node[pos+2:], 1)
		// Write the actual key and value.
		copy(node[pos+4:], keys[i])
		copy(node[pos+5:], vals[i])
	}

	// Define test cases: each search key and the expected index.
	tests := []struct {
		searchKey []byte
		expected  uint16
	}{
		{[]byte("a"), 0},      // Exact match for first key.
		{[]byte("b"), 0},      // Between "a" and "c" → index 0.
		{[]byte("c"), 1},      // Exact match for second key.
		{[]byte("d"), 1},      // Between "c" and "e" → index 1.
		{[]byte("e"), 2},      // Exact match for third key.
		{[]byte("f"), 2},      // Greater than the last key → index 2.
		{[]byte("0"), 0xFFFF}, // Less than the first key → MAX_UINT16.
	}

	for _, tt := range tests {
		idx := nodeLookupLE(node, tt.searchKey)
		if idx != tt.expected {
			t.Errorf("nodeLookupLE(%s): expected %d, got %d", tt.searchKey, tt.expected, idx)
		}
	}
}

// TestAssertInGetKey verifies that calling getKey with an invalid index (equal to nkeys)
// correctly triggers a panic.
func TestAssertInGetKey(t *testing.T) {
	node := newNode()
	node.setHeader(NodeTypeLeaf, 1)
	// Set an offset for the single key-value pair.
	node.setOffset(1, 0)
	// Write an empty key-value pair (lengths are zero).
	pos := node.kvPos(0)
	binary.LittleEndian.PutUint16(node[pos:], 0)   // key length
	binary.LittleEndian.PutUint16(node[pos+2:], 0) // value length

	// Accessing getKey with index equal to nkeys (which is out-of-bound) should panic.
	expectPanic(t, func() {
		_ = node.getKey(1)
	})
}

// TestAssertInSetPtr verifies that attempting to set a pointer at an invalid index
// (index >= nkeys) triggers a panic.
func TestAssertInSetPtr(t *testing.T) {
	node := newNode()
	node.setHeader(NodeTypeInternal, 1)
	expectPanic(t, func() {
		node.setPtr(1, 100)
	})
}

// TestMultipleKeyValuePairs is an additional test that adds multiple key-value pairs with
// variable key and value sizes. It verifies that the node correctly calculates offsets,
// stores the records, and that getKey and getVal return the expected data.
func TestMultipleKeyValuePairs(t *testing.T) {
	node := newNode()
	n := uint16(4)
	node.setHeader(NodeTypeLeaf, n)

	// Define key-value pairs with varying lengths.
	kvs := []struct {
		key string
		val string
	}{
		{"short", "val1"},
		{"a bit longer key", "value2"},
		{"key3", "a much much longer value than before"},
		{"the longest key in this test case", "v"},
	}

	// Calculate and set offsets for each key-value pair.
	currentOffset := uint16(0)
	for i, kv := range kvs {
		// Record size: 2 bytes (key length) + 2 bytes (value length) + key length + value length.
		recordSize := 2 + 2 + uint16(len(kv.key)) + uint16(len(kv.val))
		currentOffset += recordSize
		// Offsets are stored with indices starting at 1.
		node.setOffset(uint16(i+1), currentOffset)
	}

	// Write each key-value pair into the node.
	for i, kv := range kvs {
		pos := node.kvPos(uint16(i))
		binary.LittleEndian.PutUint16(node[pos:], uint16(len(kv.key)))
		binary.LittleEndian.PutUint16(node[pos+2:], uint16(len(kv.val)))
		copy(node[pos+4:], []byte(kv.key))
		copy(node[pos+4+uint16(len(kv.key)):], []byte(kv.val))
	}

	// Verify that all key-value pairs are stored and retrievable correctly.
	for i, kv := range kvs {
		retrievedKey := node.getKey(uint16(i))
		retrievedVal := node.getVal(uint16(i))
		if !bytes.Equal(retrievedKey, []byte(kv.key)) {
			t.Errorf("pair %d: expected key '%s', got '%s'", i, kv.key, string(retrievedKey))
		}
		if !bytes.Equal(retrievedVal, []byte(kv.val)) {
			t.Errorf("pair %d: expected value '%s', got '%s'", i, kv.val, string(retrievedVal))
		}
	}
}

// TestNodeSplit verifies the node splitting functionality:
// - Splitting a node into two nodes
// - Preserving all keys after split
// - Maintaining proper distribution of keys
func TestNodeSplit(t *testing.T) {
	node := make(BNode, BTREE_PAGE_SIZE)
	node.setHeader(NodeTypeLeaf, 5)

	// Insert test data
	testData := []struct {
		key   []byte
		value []byte
	}{
		{[]byte("a"), []byte("1")},
		{[]byte("b"), []byte("2")},
		{[]byte("c"), []byte("3")},
		{[]byte("d"), []byte("4")},
		{[]byte("e"), []byte("5")},
	}

	for i, data := range testData {
		nodeAppendKV(node, uint16(i), 0, data.key, data.value)
	}

	// Test splitting
	left := make(BNode, BTREE_PAGE_SIZE)
	right := make(BNode, BTREE_PAGE_SIZE)
	nodeSplit2(left, right, node)

	// Verify split results
	if left.nkeys() == 0 || right.nkeys() == 0 {
		t.Error("Split resulted in empty node")
	}

	// Verify all keys are preserved
	allKeys := make(map[string]bool)
	for i := uint16(0); i < left.nkeys(); i++ {
		allKeys[string(left.getKey(i))] = true
	}
	for i := uint16(0); i < right.nkeys(); i++ {
		allKeys[string(right.getKey(i))] = true
	}

	for _, data := range testData {
		if !allKeys[string(data.key)] {
			t.Errorf("Key %s missing after split", data.key)
		}
	}
}

// TestNodeBytes verifies the node size management:
// - Calculating total node size
// - Ensuring size constraints are met
// - Handling multiple entries
func TestNodeBytes(t *testing.T) {
	node := make(BNode, BTREE_PAGE_SIZE)
	node.setHeader(NodeTypeLeaf, 2)

	// Insert test data
	nodeAppendKV(node, 0, 0, []byte("key1"), []byte("value1"))
	nodeAppendKV(node, 1, 0, []byte("key2"), []byte("value2"))

	// Verify node size
	size := node.nbytes()
	if size == 0 {
		t.Error("Node size should not be zero")
	}
	if size > BTREE_PAGE_SIZE {
		t.Errorf("Node size %d exceeds page size %d", size, BTREE_PAGE_SIZE)
	}
}
