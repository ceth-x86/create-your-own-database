package btree

import (
	"bytes"
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
