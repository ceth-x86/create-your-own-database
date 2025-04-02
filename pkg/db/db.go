// Package db implements a simple key-value database using a B+ tree for storage
package db

import (
	"build-your-own-database/pkg/btree"
	"build-your-own-database/pkg/storage"
	"sync"
)

// DB represents the main database structure that provides thread-safe access
// to a persistent key-value store backed by a B+ tree
type DB struct {
	tree    *btree.BTree     // B+ tree for efficient key-value storage and retrieval
	storage *storage.Storage // Handles persistent storage operations on disk
	mu      sync.RWMutex     // Read-write mutex for thread-safe concurrent access
}

// NewDB creates and initializes a new database instance
// Parameters:
//   - path: The filesystem path where the database file will be stored
//
// Returns:
//   - *DB: A pointer to the initialized database
//   - error: Any error that occurred during initialization
func NewDB(path string) (*DB, error) {
	s, err := storage.NewStorage(path)
	if err != nil {
		return nil, err
	}

	db := &DB{
		storage: s,
	}

	// Initialize the B+ tree with storage callbacks for persistence
	db.tree = &btree.BTree{
		// Get callback: Reads a node from disk using its page pointer
		Get: func(ptr uint64) []byte {
			data, err := s.Read(int64(ptr), int(btree.BTREE_PAGE_SIZE))
			if err != nil {
				panic(err)
			}
			return data
		},

		// New callback: Allocates space for a new node and writes it to disk
		New: func(node []byte) uint64 {
			// Get the current file size to use as the offset for new data
			stat, err := s.File.Stat()
			if err != nil {
				panic(err)
			}
			offset := stat.Size()

			// Write the node to disk at the calculated offset
			if err := s.Write(offset, node); err != nil {
				panic(err)
			}

			return uint64(offset)
		},

		// Del callback: Handles deletion of nodes
		// Currently implements a simple strategy where deleted space is not reclaimed
		Del: func(ptr uint64) {
			// In this simple implementation, we don't actually delete data
			// We just mark the space as free for reuse
		},
	}

	return db, nil
}

// Put inserts or updates a key-value pair in the database
// Parameters:
//   - key: The key to store
//   - value: The value to associate with the key
//
// Returns:
//   - error: Any error that occurred during the operation
func (db *DB) Put(key, value []byte) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	db.tree.Insert(key, value)
	return nil
}

// Get retrieves a value from the database by its key
// Parameters:
//   - key: The key to look up
//
// Returns:
//   - []byte: The value associated with the key
//   - bool: true if the key was found, false otherwise
func (db *DB) Get(key []byte) ([]byte, bool) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	return db.tree.Search(key)
}

// Delete removes a key-value pair from the database
// Parameters:
//   - key: The key to remove
//
// Returns:
//   - error: Any error that occurred during the operation
func (db *DB) Delete(key []byte) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	db.tree.Delete(key)
	return nil
}

// Close safely shuts down the database, ensuring all data is properly saved
// Returns:
//   - error: Any error that occurred during shutdown
func (db *DB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	return db.storage.Close()
}

// Traverse walks through all key-value pairs in the database in order
// Parameters:
//   - visit: A callback function that will be called for each key-value pair
//
// The callback function receives each key-value pair in sorted order by key
func (db *DB) Traverse(visit func(key, value []byte)) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	db.tree.Traverse(visit)
}
