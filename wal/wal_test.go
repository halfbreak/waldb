package wal_test

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"halfbreak/waldb/wal"

	"github.com/stretchr/testify/assert"
)

func TestWAL_CreateError(t *testing.T) {
	_, err := wal.NewWAL("notExistentFolder")
	assert.Error(t, err)
}

func TestWAL_Append(t *testing.T) {
	wal, err := wal.NewWAL(".")
	assert.NoError(t, err)
	err = wal.Append("key1", []byte("value"))
	assert.NoError(t, err)
	err = wal.Close()
	assert.NoError(t, err)

	removeFile()
}

func BenchmarkAdd(b *testing.B) {
	wal, err := wal.NewWAL(".")
	var wg sync.WaitGroup
	assert.NoError(b, err)
	for i := range 100000 {
		wg.Add(1)
		go func() {
			err = wal.Append(fmt.Sprintf("key%d", i), []byte("value"))
			assert.NoError(b, err)
			wg.Done()
		}()
	}
	wg.Wait()
	err = wal.Close()
	assert.NoError(b, err)
	removeFile()
}

func removeFile() {
	matches, err := filepath.Glob("wal_*.db")
	if err != nil {
		log.Printf("Error finding files: %s", err.Error())
	}

	for _, file := range matches {
		err := os.Remove(file)
		if err != nil {
			log.Printf("Failed to remove file %s: %v", file, err)
		} else {
			log.Printf("Removed file %s", file)
		}
	}
}
