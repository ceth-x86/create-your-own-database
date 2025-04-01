package btree

import (
	"bytes"
	"testing"
)

// TestNodeHeader verifies the basic header operations of a B+ tree node:
// - Setting and getting node type (leaf/internal)
// - Setting and getting number of keys
func TestNodeHeader(t *testing.T) {
	// Create an empty node with default page size
	node := make(BNode, BTREE_PAGE_SIZE)

	// Test case 1: Leaf node with 5 keys
	node.setHeader(BNODE_LEAF, 5)
	if node.btype() != BNODE_LEAF {
		t.Errorf("Expected btype %d, got %d", BNODE_LEAF, node.btype())
	}
	if node.nkeys() != 5 {
		t.Errorf("Expected nkeys 5, got %d", node.nkeys())
	}

	// Test case 2: Internal node with 3 keys
	node.setHeader(BNODE_NODE, 3)
	if node.btype() != BNODE_NODE {
		t.Errorf("Expected btype %d, got %d", BNODE_NODE, node.btype())
	}
	if node.nkeys() != 3 {
		t.Errorf("Expected nkeys 3, got %d", node.nkeys())
	}
}

// TestNodePointers verifies operations with child pointers in internal nodes:
// - Setting pointers at specific indices
// - Retrieving pointers from specific indices
// - Handling multiple pointers
func TestNodePointers(t *testing.T) {
	node := make(BNode, BTREE_PAGE_SIZE)
	node.setHeader(BNODE_NODE, 3)

	// Test setting and getting pointers
	testPtrs := []uint64{100, 200, 300}
	for i, ptr := range testPtrs {
		node.setPtr(uint16(i), ptr)
		if got := node.getPtr(uint16(i)); got != ptr {
			t.Errorf("Expected pointer %d at index %d, got %d", ptr, i, got)
		}
	}
}

// TestNodeOffsets verifies the offset management functionality:
// - Setting offsets for key-value pairs
// - Retrieving offsets
// - Handling multiple offsets
func TestNodeOffsets(t *testing.T) {
	node := make(BNode, BTREE_PAGE_SIZE)
	node.setHeader(BNODE_LEAF, 3)

	// Test setting and getting offsets
	testOffsets := []uint16{1, 2, 3}
	for i, offset := range testOffsets {
		node.setOffset(uint16(i+1), offset)
		if got := node.getOffset(uint16(i + 1)); got != offset {
			t.Errorf("Expected offset %d at index %d, got %d", offset, i, got)
		}
	}
}

// TestNodeKeyValue verifies key-value pair operations:
// - Inserting single key-value pair
// - Retrieving key and value separately
// - Handling multiple key-value pairs
func TestNodeKeyValue(t *testing.T) {
	node := make(BNode, BTREE_PAGE_SIZE)
	node.setHeader(BNODE_LEAF, 2)

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

// TestNodeLookupLE verifies the Less-or-Equal lookup functionality:
// - Exact matches
// - Keys between existing entries
// - Keys before first entry
// - Keys after last entry
func TestNodeLookupLE(t *testing.T) {
	node := make(BNode, BTREE_PAGE_SIZE)
	node.setHeader(BNODE_LEAF, 3)

	// Insert keys in sorted order
	keys := [][]byte{
		[]byte("apple"),
		[]byte("banana"),
		[]byte("cherry"),
	}

	for i, key := range keys {
		nodeAppendKV(node, uint16(i), 0, key, []byte("value"))
	}

	// Test lookups
	tests := []struct {
		key      []byte
		expected uint16
	}{
		{[]byte("apple"), 0},
		{[]byte("apricot"), 0},
		{[]byte("banana"), 1},
		{[]byte("berry"), 1},
		{[]byte("cherry"), 2},
		{[]byte("date"), 2},
		{[]byte("a"), 65535},
	}

	for _, test := range tests {
		if got := nodeLookupLE(node, test.key); got != test.expected {
			t.Errorf("For key %s, expected index %d, got %d", test.key, test.expected, got)
		}
	}
}

// TestNodeSplit verifies the node splitting functionality:
// - Splitting a node into two nodes
// - Preserving all keys after split
// - Maintaining proper distribution of keys
func TestNodeSplit(t *testing.T) {
	node := make(BNode, BTREE_PAGE_SIZE)
	node.setHeader(BNODE_LEAF, 5)

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
	node.setHeader(BNODE_LEAF, 2)

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
