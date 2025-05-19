package wal

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestEntry_EncodeDecode(t *testing.T) {
	entry := &WALEntry{
		Timestamp: time.Now().UnixNano(),
		Key:       "key",
		Value:     []byte("value"),
	}

	encoded := entry.Encode()
	decodedEntry, err := Decode(encoded)
	assert.NoError(t, err)

	assert.Equal(t, entry, decodedEntry)
}
