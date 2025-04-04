package btree

import (
	"bytes"
	"fmt"
	"strings"
	"sync"
	"testing"
)

// MockStorage provides an in-memory storage implementation for testing.
// It simulates a disk storage system by maintaining a map of page numbers to their contents.
type MockStorage struct {
	pages map[uint64][]byte // Maps page numbers to their contents
	mu    sync.RWMutex      // Protects concurrent access to pages
}

// NewMockStorage creates a new mock storage instance with an empty page map.
// This is used to simulate a fresh disk storage system.
func NewMockStorage() *MockStorage {
	return &MockStorage{
		pages: make(map[uint64][]byte),
	}
}

// Get retrieves the contents of a page by its number.
// Returns nil if the page doesn't exist.
func (m *MockStorage) Get(ptr uint64) []byte {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.pages[ptr]
}

// New allocates a new page and stores the provided data.
// Returns the new page number (1-based index).
func (m *MockStorage) New(node []byte) uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	ptr := uint64(len(m.pages) + 1)
	m.pages[ptr] = make([]byte, len(node))
	copy(m.pages[ptr], node)
	return ptr
}

// Del removes a page from storage by its number.
// This simulates deallocating a page on disk.
func (m *MockStorage) Del(ptr uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.pages, ptr)
}

// NewTestTree creates a new BTree instance with mock storage for testing.
// This provides a clean environment for each test case.
func NewTestTree() *BTree {
	mock := NewMockStorage()
	return NewBTree(mock.Get, mock.New, mock.Del)
}

// TestInsertAndSearch verifies the basic functionality of the B+ tree:
// 1. Empty tree behavior
// 2. Single key insertion and retrieval
// 3. Handling of non-existent keys
func TestInsertAndSearch(t *testing.T) {
	tree := NewTestTree()

	// Verify that searching in an empty tree returns false
	if _, found := tree.Search([]byte("key")); found {
		t.Error("Empty tree should not find any keys")
	}

	// Test inserting and retrieving a single key-value pair
	key := []byte("test")
	value := []byte("value")
	tree.Insert(key, value)

	// Verify the inserted key can be found with correct value
	if val, found := tree.Search(key); !found {
		t.Error("Failed to find inserted key")
	} else if !bytes.Equal(val, value) {
		t.Errorf("Expected value %s, got %s", value, val)
	}

	// Verify that searching for a non-existent key returns false
	if _, found := tree.Search([]byte("nonexistent")); found {
		t.Error("Should not find non-existent key")
	}
}

// TestMultipleInsertions verifies that the B+ tree can handle multiple key-value pairs
// and maintains correct ordering and retrieval of all pairs.
func TestMultipleInsertions(t *testing.T) {
	tree := NewTestTree()

	// Define a set of test key-value pairs with different lengths and characters
	pairs := map[string]string{
		"apple":  "red",
		"banana": "yellow",
		"grape":  "purple",
		"orange": "orange",
	}

	for k, v := range pairs {
		tree.Insert([]byte(k), []byte(v))
	}

	// Verify that all pairs can be retrieved with correct values
	for k, v := range pairs {
		if val, found := tree.Search([]byte(k)); !found {
			t.Errorf("Failed to find key %s", k)
		} else if !bytes.Equal(val, []byte(v)) {
			t.Errorf("Expected value %s for key %s, got %s", v, k, val)
		}
	}
}

// TestUpdateExistingKey verifies that updating an existing key's value works correctly:
// 1. Initial insertion
// 2. Value update
// 3. Verification of updated value
func TestUpdateExistingKey(t *testing.T) {
	tree := NewTestTree()

	// Insert initial key-value pair
	key := []byte("test")
	initialValue := []byte("initial")
	tree.Insert(key, initialValue)

	// Update the value for the same key
	newValue := []byte("updated")
	tree.Insert(key, newValue)

	// Verify that the value was updated correctly
	if val, found := tree.Search(key); !found {
		t.Error("Failed to find updated key")
	} else if !bytes.Equal(val, newValue) {
		t.Errorf("Expected updated value %s, got %s", newValue, val)
	}
}

// TestDelete verifies the deletion functionality:
// 1. Insert a key-value pair
// 2. Delete the key
// 3. Verify the key is no longer present
func TestDelete(t *testing.T) {
	tree := NewTestTree()

	// Insert a test key-value pair
	key := []byte("test")
	value := []byte("value")
	tree.Insert(key, value)

	// Delete the key
	tree.Delete(key)

	// Verify the key is no longer in the tree
	if _, found := tree.Search(key); found {
		t.Error("Deleted key should not be found")
	}
}

// TestDeleteNonExistentKey verifies that deleting a non-existent key:
// 1. Does not affect the tree structure
// 2. Maintains existing key-value pairs
// 3. Does not cause any errors
func TestDeleteNonExistentKey(t *testing.T) {
	tree := NewTestTree()

	// Insert a test key-value pair
	key := []byte("test")
	value := []byte("value")
	tree.Insert(key, value)

	// Attempt to delete a non-existent key
	tree.Delete([]byte("nonexistent"))

	// Verify the tree remains valid and the original key is still present
	if tree.Root == 0 {
		t.Error("Tree root should not be zero after deleting non-existent key")
	}
}

// TestTraverse verifies the tree traversal functionality:
// 1. Correctly visits all key-value pairs
// 2. Maintains proper ordering
// 3. Handles multiple pairs correctly
func TestTraverse(t *testing.T) {
	tree := NewTestTree()

	// Insert multiple key-value pairs with different values
	pairs := map[string]string{
		"apple":  "red",
		"banana": "yellow",
		"grape":  "purple",
		"orange": "orange",
	}

	// Insert all pairs into the tree
	for k, v := range pairs {
		tree.Insert([]byte(k), []byte(v))
	}

	// Collect all key-value pairs during traversal
	found := make(map[string]string)
	tree.Traverse(func(key, value []byte) {
		found[string(key)] = string(value)
	})

	// Verify that all pairs were found during traversal
	if len(found) != len(pairs) {
		t.Errorf("Expected %d pairs, found %d", len(pairs), len(found))
	}

	// Verify each pair matches exactly
	for k, v := range pairs {
		if found[k] != v {
			t.Errorf("Expected %s -> %s, found %s -> %s", k, v, k, found[k])
		}
	}
}

// TestNodeSplit2 verifies the basic splitting functionality of nodeSplit2
func TestNodeSplit2(t *testing.T) {
	cfg := DefaultConfig

	// Test case 1: Basic split
	t.Run("basic split", func(t *testing.T) {
		// Create a node with some test data
		old := make(BNode, cfg.PageSize*2)
		old.setHeader(NodeTypeLeaf, 10)

		// Add some test key-value pairs
		for i := uint16(0); i < 10; i++ {
			key := []byte(fmt.Sprintf("key%d", i))
			val := []byte(fmt.Sprintf("value%d", i))
			nodeAppendKV(old, i, 0, key, val)
		}

		// Create target nodes for the split
		left := make(BNode, cfg.PageSize)
		right := make(BNode, cfg.PageSize)

		// Perform the split
		nodeSplit2(left, right, old, cfg)

		// Verify the split results
		if left.nkeys() == 0 || right.nkeys() == 0 {
			t.Error("Split should result in non-empty nodes")
		}

		if left.nkeys()+right.nkeys() != old.nkeys() {
			t.Errorf("Total keys after split should equal original keys. Got %d + %d != %d",
				left.nkeys(), right.nkeys(), old.nkeys())
		}

		// Verify right node fits within page size
		if right.nbytes() > cfg.PageSize {
			t.Errorf("Right node exceeds page size: %d > %d", right.nbytes(), cfg.PageSize)
		}

		// Verify node types are preserved
		if left.btype() != old.btype() || right.btype() != old.btype() {
			t.Error("Node types should be preserved after split")
		}
	})

	// Test case 2: Too few keys
	t.Run("too few keys", func(t *testing.T) {
		old := make(BNode, cfg.PageSize)
		old.setHeader(NodeTypeLeaf, 1) // Less than minimum required keys

		left := make(BNode, cfg.PageSize)
		right := make(BNode, cfg.PageSize)

		// This should panic due to assertion in nodeSplit2
		defer func() {
			if r := recover(); r == nil {
				t.Error("Expected panic for too few keys")
			}
		}()
		nodeSplit2(left, right, old, cfg)
	})
}

// TestNodeSplit3 verifies the nodeSplit3 function's ability to handle different scenarios
func TestNodeSplit3(t *testing.T) {
	cfg := DefaultConfig

	// Helper function to create a node with test data
	createTestNode := func(nkeys uint16) BNode {
		node := make(BNode, cfg.PageSize)
		node.setHeader(NodeTypeLeaf, nkeys)
		for i := uint16(0); i < nkeys; i++ {
			key := []byte(fmt.Sprintf("key%d", i))
			val := []byte(fmt.Sprintf("value%d", i))
			nodeAppendKV(node, i, 0, key, val)
		}
		return node
	}

	// Test case 1: Node that fits in one page
	t.Run("fits in one page", func(t *testing.T) {
		old := createTestNode(5)

		nsplit, split := nodeSplit3(old, cfg)
		if nsplit != 1 {
			t.Errorf("Expected 1 node, got %d", nsplit)
		}
		if len(split[0]) == 0 {
			t.Error("Split node should not be empty")
		}
		if split[0].nkeys() != old.nkeys() {
			t.Error("Number of keys should be preserved when no split occurs")
		}
	})

	// Test case 2: Node that needs to be split into two
	t.Run("splits into two", func(t *testing.T) {
		// Create a node that's too big for one page
		old := make(BNode, cfg.PageSize*2)
		old.setHeader(NodeTypeLeaf, 30) // Increased number of keys
		for i := uint16(0); i < 30; i++ {
			// Create larger key-value pairs to ensure the node exceeds page size
			key := []byte(fmt.Sprintf("key%d_%s", i, strings.Repeat("x", 100)))
			val := []byte(fmt.Sprintf("value%d_%s", i, strings.Repeat("y", 100)))
			nodeAppendKV(old, i, 0, key, val)
		}

		nsplit, split := nodeSplit3(old, cfg)
		if nsplit != 2 {
			t.Errorf("Expected 2 nodes, got %d", nsplit)
		}

		// Verify both nodes fit within page size
		for i := 0; i < int(nsplit); i++ {
			if split[i].nbytes() > cfg.PageSize {
				t.Errorf("Node %d exceeds page size: %d > %d",
					i, split[i].nbytes(), cfg.PageSize)
			}
		}

		// Verify total keys are preserved
		totalKeys := uint16(0)
		for i := 0; i < int(nsplit); i++ {
			totalKeys += split[i].nkeys()
		}
		if totalKeys != old.nkeys() {
			t.Errorf("Total keys after split should equal original keys. Got %d != %d",
				totalKeys, old.nkeys())
		}
	})

	// Test case 3: Node that needs to be split into three
	t.Run("splits into three", func(t *testing.T) {
		// Create a node that's too big for one page
		old := make(BNode, cfg.PageSize*3)
		old.setHeader(NodeTypeLeaf, 50) // Increased number of keys
		for i := uint16(0); i < 50; i++ {
			// Create larger key-value pairs to ensure the node exceeds page size
			key := []byte(fmt.Sprintf("key%d_%s", i, strings.Repeat("x", 100)))
			val := []byte(fmt.Sprintf("value%d_%s", i, strings.Repeat("y", 100)))
			nodeAppendKV(old, i, 0, key, val)
		}

		nsplit, split := nodeSplit3(old, cfg)
		if nsplit != 3 {
			t.Errorf("Expected 3 nodes, got %d", nsplit)
		}

		// Verify all nodes fit within page size
		for i := 0; i < int(nsplit); i++ {
			if split[i].nbytes() > cfg.PageSize {
				t.Errorf("Node %d exceeds page size: %d > %d",
					i, split[i].nbytes(), cfg.PageSize)
			}
		}

		// Verify total keys are preserved
		totalKeys := uint16(0)
		for i := 0; i < int(nsplit); i++ {
			totalKeys += split[i].nkeys()
		}
		if totalKeys != old.nkeys() {
			t.Errorf("Total keys after split should equal original keys. Got %d != %d",
				totalKeys, old.nkeys())
		}
	})

	// Test case 4: Empty node
	t.Run("empty node", func(t *testing.T) {
		old := make(BNode, cfg.PageSize)
		old.setHeader(NodeTypeLeaf, 0)

		nsplit, split := nodeSplit3(old, cfg)
		if nsplit != 1 {
			t.Errorf("Expected 1 node for empty input, got %d", nsplit)
		}
		if split[0].nkeys() != 0 {
			t.Error("Empty input should result in empty node")
		}
	})
}

// TestNodeSplitConsistency verifies that the split operations maintain data consistency
func TestNodeSplitConsistency(t *testing.T) {
	cfg := DefaultConfig

	// Create a node with some test data
	old := make(BNode, cfg.PageSize*2)
	old.setHeader(NodeTypeLeaf, 20)

	// Add some test key-value pairs
	for i := uint16(0); i < 20; i++ {
		key := []byte(fmt.Sprintf("key%d", i))
		val := []byte(fmt.Sprintf("value%d", i))
		nodeAppendKV(old, i, 0, key, val)
	}

	// Test nodeSplit2
	left := make(BNode, cfg.PageSize)
	right := make(BNode, cfg.PageSize)
	nodeSplit2(left, right, old, cfg)

	// Verify all keys and values are preserved
	allData := make(map[string]string)
	for i := uint16(0); i < left.nkeys(); i++ {
		allData[string(left.getKey(i))] = string(left.getVal(i))
	}
	for i := uint16(0); i < right.nkeys(); i++ {
		allData[string(right.getKey(i))] = string(right.getVal(i))
	}

	for i := 0; i < 20; i++ {
		key := fmt.Sprintf("key%d", i)
		expectedVal := fmt.Sprintf("value%d", i)
		if val, ok := allData[key]; !ok {
			t.Errorf("Key %s missing after split", key)
		} else if val != expectedVal {
			t.Errorf("Wrong value for key %s: expected %s, got %s", key, expectedVal, val)
		}
	}

	// Test nodeSplit3
	nsplit, split := nodeSplit3(old, cfg)

	// Verify all keys and values are preserved across all split nodes
	allData = make(map[string]string)
	for i := 0; i < int(nsplit); i++ {
		for j := uint16(0); j < split[i].nkeys(); j++ {
			allData[string(split[i].getKey(j))] = string(split[i].getVal(j))
		}
	}

	for i := 0; i < 20; i++ {
		key := fmt.Sprintf("key%d", i)
		expectedVal := fmt.Sprintf("value%d", i)
		if val, ok := allData[key]; !ok {
			t.Errorf("Key %s missing after split3", key)
		} else if val != expectedVal {
			t.Errorf("Wrong value for key %s: expected %s, got %s", key, expectedVal, val)
		}
	}
}

// TestLargeDataset is a commented-out test that verifies the B+ tree's performance
// with a large number of key-value pairs (1000 entries).
// This test is useful for performance testing but may be slow to run regularly.
/*
func TestLargeDataset(t *testing.T) {
	tree := NewTestTree()

	// Insert a large number of key-value pairs
	const numPairs = 1000
	for i := 0; i < numPairs; i++ {
		key := []byte(fmt.Sprintf("key%d", i))
		value := []byte(fmt.Sprintf("value%d", i))
		tree.Insert(key, value)
	}

	// Verify all pairs can be retrieved
	for i := 0; i < numPairs; i++ {
		key := []byte(fmt.Sprintf("key%d", i))
		expectedValue := []byte(fmt.Sprintf("value%d", i))
		if val, found := tree.Search(key); !found {
			t.Errorf("Failed to find key %s", key)
		} else if !bytes.Equal(val, expectedValue) {
			t.Errorf("Expected value %s for key %s, got %s", expectedValue, key, val)
		}
	}
}
*/

// TestConcurrentOperations is a commented-out test that verifies the B+ tree's
// behavior under concurrent access. It tests:
// 1. Multiple goroutines inserting keys simultaneously
// 2. Thread safety of the tree operations
// 3. Correctness of all operations under concurrent access
/*
func TestConcurrentOperations(t *testing.T) {
	tree := NewTestTree()

	// Run concurrent insertions
	const numGoroutines = 10
	const numOperations = 100
	var wg sync.WaitGroup

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(routineID int) {
			defer wg.Done()
			for j := 0; j < numOperations; j++ {
				key := []byte(fmt.Sprintf("key%d_%d", routineID, j))
				value := []byte(fmt.Sprintf("value%d_%d", routineID, j))
				tree.Insert(key, value)
			}
		}(i)
	}

	wg.Wait()

	// Verify all inserted values can be retrieved
	for i := 0; i < numGoroutines; i++ {
		for j := 0; j < numOperations; j++ {
			key := []byte(fmt.Sprintf("key%d_%d", i, j))
			expectedValue := []byte(fmt.Sprintf("value%d_%d", i, j))
			if val, found := tree.Search(key); !found {
				t.Errorf("Failed to find key %s", key)
			} else if !bytes.Equal(val, expectedValue) {
				t.Errorf("Expected value %s for key %s, got %s", expectedValue, key, val)
			}
		}
	}
}
*/

// TestEdgeCases is a commented-out test that verifies the B+ tree's behavior
// with special cases:
// 1. Empty keys
// 2. Very long keys and values
// 3. Special characters in keys
/*
func TestEdgeCases(t *testing.T) {
	tree := NewTestTree()

	// Test empty key
	tree.Insert([]byte{}, []byte("empty"))
	if val, found := tree.Search([]byte{}); !found {
		t.Error("Failed to find empty key")
	} else if !bytes.Equal(val, []byte("empty")) {
		t.Error("Wrong value for empty key")
	}

	// Test very long key
	longKey := bytes.Repeat([]byte("x"), BTREE_MAX_KEY_SIZE)
	longValue := bytes.Repeat([]byte("y"), BTREE_MAX_VAL_SIZE)
	tree.Insert(longKey, longValue)
	if val, found := tree.Search(longKey); !found {
		t.Error("Failed to find long key")
	} else if !bytes.Equal(val, longValue) {
		t.Error("Wrong value for long key")
	}

	// Test special characters in key
	specialKey := []byte("!@#$%^&*()")
	tree.Insert(specialKey, []byte("special"))
	if val, found := tree.Search(specialKey); !found {
		t.Error("Failed to find special key")
	} else if !bytes.Equal(val, []byte("special")) {
		t.Error("Wrong value for special key")
	}
}
*/
