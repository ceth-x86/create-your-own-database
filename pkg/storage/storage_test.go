package storage

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func TestNewStorage(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "test.db")

	storage, err := NewStorage(path)
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}
	defer storage.Close()

	// Verify storage file exists
	if _, err := os.Stat(path); os.IsNotExist(err) {
		t.Error("Storage file was not created")
	}
}

func TestReadWrite(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "test.db")

	storage, err := NewStorage(path)
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}
	defer storage.Close()

	// Test data
	data := []byte("test data")
	offset := int64(0)

	// Write data
	if err := storage.Write(offset, data); err != nil {
		t.Fatalf("Failed to write data: %v", err)
	}

	// Read data back
	readData, err := storage.Read(offset, len(data))
	if err != nil {
		t.Fatalf("Failed to read data: %v", err)
	}

	if !bytes.Equal(readData, data) {
		t.Errorf("Expected data %s, got %s", data, readData)
	}
}

func TestConcurrentReadWrite(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "test.db")

	storage, err := NewStorage(path)
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}
	defer storage.Close()

	// Test concurrent writes
	const numGoroutines = 10
	const numOperations = 100
	var wg sync.WaitGroup

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(routineID int) {
			defer wg.Done()
			for j := 0; j < numOperations; j++ {
				offset := int64(routineID*numOperations+j) * 100 // Use fixed size blocks
				data := []byte(fmt.Sprintf("data_%d_%d", routineID, j))
				if err := storage.Write(offset, data); err != nil {
					t.Errorf("Failed to write data: %v", err)
				}
			}
		}(i)
	}

	wg.Wait()

	// Verify all writes
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

func TestClose(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "test.db")

	storage, err := NewStorage(path)
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}

	// Write some data
	data := []byte("test data")
	offset := int64(0)
	if err := storage.Write(offset, data); err != nil {
		t.Fatalf("Failed to write data: %v", err)
	}

	// Close storage
	if err := storage.Close(); err != nil {
		t.Fatalf("Failed to close storage: %v", err)
	}

	// Verify file still exists
	if _, err := os.Stat(path); os.IsNotExist(err) {
		t.Error("Storage file was deleted after close")
	}
}

func TestLargeData(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "test.db")

	storage, err := NewStorage(path)
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}
	defer storage.Close()

	// Create large data (1MB)
	largeData := bytes.Repeat([]byte("x"), 1024*1024)
	offset := int64(0)

	// Write large data
	if err := storage.Write(offset, largeData); err != nil {
		t.Fatalf("Failed to write large data: %v", err)
	}

	// Read large data back
	readData, err := storage.Read(offset, len(largeData))
	if err != nil {
		t.Fatalf("Failed to read large data: %v", err)
	}

	if !bytes.Equal(readData, largeData) {
		t.Error("Large data mismatch")
	}
}
