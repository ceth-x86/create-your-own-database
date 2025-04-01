package testutil

import (
	"build-your-own-database/pkg/btree"
	"sync"
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
func NewTestTree() *btree.BTree {
	mock := NewMockStorage()
	return &btree.BTree{
		Get: mock.Get,
		New: mock.New,
		Del: mock.Del,
	}
}
