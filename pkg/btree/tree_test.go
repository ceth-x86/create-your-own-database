package btree

import (
	"bytes"
	"fmt"
	"sync"
	"testing"
)

// MockStorage provides an in-memory storage implementation for testing
type MockStorage struct {
	pages map[uint64][]byte
	mu    sync.RWMutex
}

func NewMockStorage() *MockStorage {
	return &MockStorage{
		pages: make(map[uint64][]byte),
	}
}

func (m *MockStorage) Get(ptr uint64) []byte {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.pages[ptr]
}

func (m *MockStorage) New(node []byte) uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	ptr := uint64(len(m.pages) + 1)
	m.pages[ptr] = make([]byte, len(node))
	copy(m.pages[ptr], node)
	return ptr
}

func (m *MockStorage) Del(ptr uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.pages, ptr)
}

// NewTestTree creates a new BTree instance with mock storage
func NewTestTree() *BTree {
	mock := NewMockStorage()
	return &BTree{
		Get: mock.Get,
		New: mock.New,
		Del: mock.Del,
	}
}

func TestInsertAndSearch(t *testing.T) {
	tree := NewTestTree()

	// Test empty tree
	if _, found := tree.Search([]byte("key")); found {
		t.Error("Empty tree should not find any keys")
	}

	// Test single key insertion and retrieval
	key := []byte("test")
	value := []byte("value")
	tree.Insert(key, value)

	if val, found := tree.Search(key); !found {
		t.Error("Failed to find inserted key")
	} else if !bytes.Equal(val, value) {
		t.Errorf("Expected value %s, got %s", value, val)
	}

	// Test non-existent key
	if _, found := tree.Search([]byte("nonexistent")); found {
		t.Error("Should not find non-existent key")
	}
}

func TestMultipleInsertions(t *testing.T) {
	tree := NewTestTree()

	// Insert multiple key-value pairs
	pairs := map[string]string{
		"apple":  "red",
		"banana": "yellow",
		"grape":  "purple",
		"orange": "orange",
	}

	for k, v := range pairs {
		tree.Insert([]byte(k), []byte(v))
	}

	// Verify all pairs can be retrieved
	for k, v := range pairs {
		if val, found := tree.Search([]byte(k)); !found {
			t.Errorf("Failed to find key %s", k)
		} else if !bytes.Equal(val, []byte(v)) {
			t.Errorf("Expected value %s for key %s, got %s", v, k, val)
		}
	}
}

func TestUpdateExistingKey(t *testing.T) {
	tree := NewTestTree()

	// Insert initial value
	key := []byte("test")
	initialValue := []byte("initial")
	tree.Insert(key, initialValue)

	// Update the value
	newValue := []byte("updated")
	tree.Insert(key, newValue)

	// Verify the value was updated
	if val, found := tree.Search(key); !found {
		t.Error("Failed to find updated key")
	} else if !bytes.Equal(val, newValue) {
		t.Errorf("Expected updated value %s, got %s", newValue, val)
	}
}

func TestDelete(t *testing.T) {
	tree := NewTestTree()

	// Insert a key-value pair
	key := []byte("test")
	value := []byte("value")
	tree.Insert(key, value)

	// Delete the key
	tree.Delete(key)

	// Verify the key is deleted
	if _, found := tree.Search(key); found {
		t.Error("Deleted key should not be found")
	}
}

func TestDeleteNonExistentKey(t *testing.T) {
	tree := NewTestTree()

	// Insert a key-value pair
	key := []byte("test")
	value := []byte("value")
	tree.Insert(key, value)

	// Delete a non-existent key
	tree.Delete([]byte("nonexistent"))

	// Verify the tree is still valid
	if tree.Root == 0 {
		t.Error("Tree root should not be zero after deleting non-existent key")
	}
}

func TestTraverse(t *testing.T) {
	tree := NewTestTree()

	// Insert multiple key-value pairs
	pairs := map[string]string{
		"apple":  "red",
		"banana": "yellow",
		"grape":  "purple",
		"orange": "orange",
	}

	for k, v := range pairs {
		tree.Insert([]byte(k), []byte(v))
	}

	// Collect all key-value pairs during traversal
	found := make(map[string]string)
	tree.Traverse(func(key, value []byte) {
		found[string(key)] = string(value)
	})

	// Verify all pairs were found
	if len(found) != len(pairs) {
		t.Errorf("Expected %d pairs, found %d", len(pairs), len(found))
	}

	for k, v := range pairs {
		if found[k] != v {
			t.Errorf("Expected %s -> %s, found %s -> %s", k, v, k, found[k])
		}
	}
}

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
