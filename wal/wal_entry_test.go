package wal_test

import (
	"testing"
	"time"

	"halfbreak/waldb/wal"

	"github.com/stretchr/testify/assert"
)

func TestEntry_EncodeDecode(t *testing.T) {
	entry := &wal.WALEntry{
		Timestamp: time.Now().UnixNano(),
		Key:       "key",
		Value:     []byte("value"),
	}

	encoded := entry.Encode()
	decodedEntry, err := wal.Decode(encoded)
	assert.NoError(t, err)

	assert.Equal(t, entry, decodedEntry)
}
