package db

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestNewDB(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "test.db")

	database, err := NewDB(path)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer database.Close()

	// Verify database file exists
	if _, err := os.Stat(path); os.IsNotExist(err) {
		t.Error("Database file was not created")
	}
}

func TestPutAndGet(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "test.db")

	database, err := NewDB(path)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer database.Close()

	// Test data
	key := []byte("test_key")
	value := []byte("test_value")

	// Put value
	if err := database.Put(key, value); err != nil {
		t.Fatalf("Failed to put value: %v", err)
	}

	// Get value
	got, found := database.Get(key)
	if !found {
		t.Error("Failed to get value")
	}
	if !bytes.Equal(got, value) {
		t.Errorf("Expected value %s, got %s", value, got)
	}
}

func TestDelete(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "test.db")

	database, err := NewDB(path)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer database.Close()

	// Insert test data
	key := []byte("test_key")
	value := []byte("test_value")
	if err := database.Put(key, value); err != nil {
		t.Fatalf("Failed to put value: %v", err)
	}

	// Delete key
	if err := database.Delete(key); err != nil {
		t.Fatalf("Failed to delete key: %v", err)
	}

	// Verify deletion
	if _, found := database.Get(key); found {
		t.Error("Deleted key still exists")
	}
}

func TestTraverse(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "test.db")

	database, err := NewDB(path)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer database.Close()

	// Test data
	pairs := map[string]string{
		"apple":  "red",
		"banana": "yellow",
		"grape":  "purple",
		"orange": "orange",
	}

	// Insert data
	for k, v := range pairs {
		if err := database.Put([]byte(k), []byte(v)); err != nil {
			t.Fatalf("Failed to put value: %v", err)
		}
	}

	// Collect data during traversal
	found := make(map[string]string)
	database.Traverse(func(key, value []byte) {
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

func TestUpdateExistingKey(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "test.db")

	database, err := NewDB(path)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer database.Close()

	// Insert initial value
	key := []byte("test_key")
	initialValue := []byte("initial_value")
	if err := database.Put(key, initialValue); err != nil {
		t.Fatalf("Failed to put initial value: %v", err)
	}

	// Update value
	newValue := []byte("updated_value")
	if err := database.Put(key, newValue); err != nil {
		t.Fatalf("Failed to update value: %v", err)
	}

	// Verify update
	got, found := database.Get(key)
	if !found {
		t.Error("Failed to get updated value")
	}
	if !bytes.Equal(got, newValue) {
		t.Errorf("Expected value %s, got %s", newValue, got)
	}
}

func TestLargeDataset(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "test.db")

	database, err := NewDB(path)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer database.Close()

	// Insert large dataset
	const numPairs = 1000
	for i := 0; i < numPairs; i++ {
		key := []byte(fmt.Sprintf("key%d", i))
		value := []byte(fmt.Sprintf("value%d", i))
		if err := database.Put(key, value); err != nil {
			t.Fatalf("Failed to put value: %v", err)
		}
	}

	// Verify all pairs
	for i := 0; i < numPairs; i++ {
		key := []byte(fmt.Sprintf("key%d", i))
		expectedValue := []byte(fmt.Sprintf("value%d", i))
		got, found := database.Get(key)
		if !found {
			t.Errorf("Failed to find key %s", key)
		}
		if !bytes.Equal(got, expectedValue) {
			t.Errorf("Expected value %s for key %s, got %s", expectedValue, key, got)
		}
	}
}

func TestEdgeCases(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "test.db")

	database, err := NewDB(path)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer database.Close()

	// Test empty key
	if err := database.Put([]byte{}, []byte("empty")); err != nil {
		t.Fatalf("Failed to put empty key: %v", err)
	}
	if val, found := database.Get([]byte{}); !found {
		t.Error("Failed to find empty key")
	} else if !bytes.Equal(val, []byte("empty")) {
		t.Error("Wrong value for empty key")
	}

	// Test very long key
	longKey := bytes.Repeat([]byte("x"), 1000)
	longValue := bytes.Repeat([]byte("y"), 3000)
	if err := database.Put(longKey, longValue); err != nil {
		t.Fatalf("Failed to put long key: %v", err)
	}
	if val, found := database.Get(longKey); !found {
		t.Error("Failed to find long key")
	} else if !bytes.Equal(val, longValue) {
		t.Error("Wrong value for long key")
	}

	// Test special characters in key
	specialKey := []byte("!@#$%^&*()")
	if err := database.Put(specialKey, []byte("special")); err != nil {
		t.Fatalf("Failed to put special key: %v", err)
	}
	if val, found := database.Get(specialKey); !found {
		t.Error("Failed to find special key")
	} else if !bytes.Equal(val, []byte("special")) {
		t.Error("Wrong value for special key")
	}
}
