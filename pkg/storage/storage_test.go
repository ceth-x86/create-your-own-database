// Package storage_test provides comprehensive tests for the storage package
// It verifies the functionality, thread-safety, and reliability of the Storage implementation
package storage

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

// TestNewStorage verifies the creation and initialization of a new Storage instance
// It checks:
// 1. Successful creation of a new storage instance
// 2. Proper file creation at the specified path
// 3. Cleanup of resources after test completion
func TestNewStorage(t *testing.T) {
	// Create a temporary directory for test files
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "test.db")

	// Create new storage instance
	storage, err := NewStorage(path)
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}
	defer storage.Close()

	// Verify that the storage file was actually created
	if _, err := os.Stat(path); os.IsNotExist(err) {
		t.Error("Storage file was not created")
	}
}

// TestReadWrite verifies basic read and write operations
// It tests:
// 1. Writing data to the storage
// 2. Reading back the written data
// 3. Data integrity verification
func TestReadWrite(t *testing.T) {
	// Set up test environment
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "test.db")

	storage, err := NewStorage(path)
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}
	defer storage.Close()

	// Prepare test data
	data := []byte("test data")
	offset := int64(0)

	// Test write operation
	if err := storage.Write(offset, data); err != nil {
		t.Fatalf("Failed to write data: %v", err)
	}

	// Test read operation
	readData, err := storage.Read(offset, len(data))
	if err != nil {
		t.Fatalf("Failed to read data: %v", err)
	}

	// Verify data integrity
	if !bytes.Equal(readData, data) {
		t.Errorf("Expected data %s, got %s", data, readData)
	}
}

// TestConcurrentReadWrite verifies thread-safety of the Storage implementation
// It tests:
// 1. Concurrent write operations from multiple goroutines
// 2. Data consistency under concurrent access
// 3. Proper synchronization using RWMutex
func TestConcurrentReadWrite(t *testing.T) {
	// Set up test environment
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "test.db")

	storage, err := NewStorage(path)
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}
	defer storage.Close()

	// Test configuration
	const numGoroutines = 10  // Number of concurrent goroutines
	const numOperations = 100 // Number of operations per goroutine
	var wg sync.WaitGroup     // WaitGroup for goroutine synchronization

	// Launch concurrent write operations
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(routineID int) {
			defer wg.Done()
			// Each goroutine performs multiple write operations
			for j := 0; j < numOperations; j++ {
				// Use fixed-size blocks to prevent overlap
				offset := int64(routineID*numOperations+j) * 100
				data := []byte(fmt.Sprintf("data_%d_%d", routineID, j))
				if err := storage.Write(offset, data); err != nil {
					t.Errorf("Failed to write data: %v", err)
				}
			}
		}(i)
	}

	// Wait for all goroutines to complete
	wg.Wait()

	// Verify all writes were successful
	for i := 0; i < numGoroutines; i++ {
		for j := 0; j < numOperations; j++ {
			offset := int64(i*numOperations+j) * 100
			expectedData := []byte(fmt.Sprintf("data_%d_%d", i, j))
			readData, err := storage.Read(offset, len(expectedData))
			if err != nil {
				t.Errorf("Failed to read data: %v", err)
			}
			if !bytes.Equal(readData, expectedData) {
				t.Errorf("Expected data %s, got %s", expectedData, readData)
			}
		}
	}
}

// TestClose verifies proper cleanup and resource management
// It tests:
// 1. Successful closure of the storage
// 2. File persistence after closure
// 3. Proper cleanup of resources
func TestClose(t *testing.T) {
	// Set up test environment
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "test.db")

	storage, err := NewStorage(path)
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}

	// Write test data before closing
	data := []byte("test data")
	offset := int64(0)
	if err := storage.Write(offset, data); err != nil {
		t.Fatalf("Failed to write data: %v", err)
	}

	// Test storage closure
	if err := storage.Close(); err != nil {
		t.Fatalf("Failed to close storage: %v", err)
	}

	// Verify file still exists after closure
	if _, err := os.Stat(path); os.IsNotExist(err) {
		t.Error("Storage file was deleted after close")
	}
}

// TestLargeData verifies handling of large data blocks
// It tests:
// 1. Writing large data blocks (1MB)
// 2. Reading large data blocks
// 3. Data integrity for large operations
func TestLargeData(t *testing.T) {
	// Set up test environment
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "test.db")

	storage, err := NewStorage(path)
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}
	defer storage.Close()

	// Create large test data (1MB)
	largeData := bytes.Repeat([]byte("x"), 1024*1024)
	offset := int64(0)

	// Test writing large data
	if err := storage.Write(offset, largeData); err != nil {
		t.Fatalf("Failed to write large data: %v", err)
	}

	// Test reading large data
	readData, err := storage.Read(offset, len(largeData))
	if err != nil {
		t.Fatalf("Failed to read large data: %v", err)
	}

	// Verify data integrity
	if !bytes.Equal(readData, largeData) {
		t.Error("Large data mismatch")
	}
}
