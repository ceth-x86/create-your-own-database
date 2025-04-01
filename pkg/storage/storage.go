package storage

import (
	"os"
	"path/filepath"
	"sync"
)

type Storage struct {
	File *os.File
	mu   sync.RWMutex
}

func NewStorage(path string) (*Storage, error) {
	// Create directory if it doesn't exist
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return nil, err
	}

	// Open or create the file
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}

	return &Storage{
		File: file,
	}, nil
}

func (s *Storage) Read(offset int64, size int) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	data := make([]byte, size)
	_, err := s.File.ReadAt(data, offset)
	return data, err
}

func (s *Storage) Write(offset int64, data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	_, err := s.File.WriteAt(data, offset)
	return err
}

func (s *Storage) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.File.Close()
}
