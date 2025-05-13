package main

import (
	"fmt"
	"os"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAdd(t *testing.T) {
	wal, err := NewWAL("test.db")
	assert.NoError(t, err)
	wal.Append("key1", []byte("value"))
	wal.Close()
	//assert.Equal(t, 5, Add(2, 3))

	removeFile()
}

func BenchmarkAdd(b *testing.B) {
	wal, err := NewWAL("test.db")
	var wg sync.WaitGroup
	assert.NoError(b, err)
	for i := range 100000 {
		wg.Add(1)
		go func() {
			wal.Append(fmt.Sprintf("key%d", i), []byte("value"))
			wg.Done()
		}()
	}
	wg.Wait()
	wal.Close()
	removeFile()
}

func removeFile() {
	err := os.Remove("test.db")
	if err != nil {
		fmt.Println("Error deleting file:", err)
	} else {
		fmt.Println("File deleted successfully")
	}
}
