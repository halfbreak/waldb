package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func setupTempFolder(t *testing.T) string {
	t.Helper()
	tmpDir := t.TempDir()
	return tmpDir
}

func TestNewWalSegment(t *testing.T) {
	tmp := setupTempFolder(t)
	segment, err := NewWalSegment(tmp, 1)
	assert.NoError(t, err)
	defer segment.Close()

	expectedFile := filepath.Join(tmp, "wal_1.db")
	_, err = os.Stat(expectedFile)
	assert.NoError(t, err)

	assert.Equal(t, 0, segment.offset)
}

func TestAppend(t *testing.T) {
	tmp := setupTempFolder(t)
	segment, err := NewWalSegment(tmp, 0)
	assert.NoError(t, err)
	defer segment.Close()

	data := []byte("hello wal")
	segment.Append(data)
	assert.Equal(t, len(data), segment.offset)

	actual := (*segment.mmappedData)[:len(data)]
	assert.Equal(t, string(data), string(actual))
}

func TestIsFull(t *testing.T) {
	tmp := setupTempFolder(t)
	segment, err := NewWalSegment(tmp, 0)
	assert.NoError(t, err)
	defer segment.Close()

	segment.offset = mmapSize - 5

	data := []byte("123456") // 6 bytes
	assert.Equal(t, true, segment.IsFull(data))

	data = []byte("1234") // 4 bytes
	assert.Equal(t, false, segment.IsFull(data))
}

func TestFlushAndClose(t *testing.T) {
	tmp := setupTempFolder(t)
	segment, err := NewWalSegment(tmp, 0)
	assert.NoError(t, err)

	segment.Append([]byte("flushed content"))

	err = segment.Flush()
	assert.NoError(t, err)

	err = segment.Close()
	assert.NoError(t, err)
}
