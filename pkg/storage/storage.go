// Package storage provides a thread-safe interface for file operations
// It implements basic storage operations with concurrent access support
package storage

import (
	"os"
	"path/filepath"
	"sync"
)

// Storage represents a thread-safe file storage handler
// It provides concurrent read/write operations to a single file
type Storage struct {
	File *os.File     // Underlying file descriptor for I/O operations
	mu   sync.RWMutex // Read-Write mutex for thread-safe file access
}

// NewStorage creates and initializes a new Storage instance
// Parameters:
//   - path: The file path where the storage will be created/opened
//
// Returns:
//   - *Storage: Pointer to the new Storage instance
//   - error: Any error that occurred during creation
//
// The function will:
//  1. Create all necessary directories in the path
//  2. Create or open the file with read/write permissions
//  3. Return a configured Storage instance
func NewStorage(path string) (*Storage, error) {
	// Create all directories in the path if they don't exist
	// Uses 0755 permissions: rwx for owner, rx for group and others
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return nil, err
	}

	// Open or create the file with read-write permissions
	// O_RDWR: Open for reading and writing
	// O_CREATE: Create file if it doesn't exist
	// 0644 permissions: rw for owner, r for group and others
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}

	return &Storage{
		File: file,
	}, nil
}

// Read performs a thread-safe read operation from the storage file
// Parameters:
//   - offset: Position in the file to start reading from
//   - size: Number of bytes to read
//
// Returns:
//   - []byte: The read data
//   - error: Any error that occurred during reading
//
// This method is thread-safe and allows multiple concurrent reads
func (s *Storage) Read(offset int64, size int) ([]byte, error) {
	s.mu.RLock()         // Acquire read lock - allows multiple concurrent reads
	defer s.mu.RUnlock() // Ensure lock is released after function returns

	data := make([]byte, size)
	_, err := s.File.ReadAt(data, offset)
	return data, err
}

// Write performs a thread-safe write operation to the storage file
// Parameters:
//   - offset: Position in the file to start writing at
//   - data: Bytes to write to the file
//
// Returns:
//   - error: Any error that occurred during writing
//
// This method is thread-safe and ensures exclusive access during writing
func (s *Storage) Write(offset int64, data []byte) error {
	s.mu.Lock()         // Acquire exclusive lock - only one writer at a time
	defer s.mu.Unlock() // Ensure lock is released after function returns

	_, err := s.File.WriteAt(data, offset)
	return err
}

// Close safely closes the storage file
// This method ensures thread-safe closure of the file handle
//
// Returns:
//   - error: Any error that occurred during closing
//
// This method should be called when the storage is no longer needed
// to free up system resources
func (s *Storage) Close() error {
	s.mu.Lock()         // Acquire exclusive lock before closing
	defer s.mu.Unlock() // Ensure lock is released after function returns

	return s.File.Close()
}
