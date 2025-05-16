package main

import (
	"encoding/binary"
	"errors"
)

type WALEntry struct {
	Timestamp int64
	Key       string
	Value     []byte
	done      chan error
}

func (e *WALEntry) Encode() []byte {
	keyBytes := []byte(e.Key)
	keyLen := uint32(len(keyBytes))
	valLen := uint32(len(e.Value))

	buf := make([]byte, 8+4+len(keyBytes)+4+len(e.Value)) // timestamp + keyLen + key + valLen + val

	binary.BigEndian.PutUint64(buf[0:], uint64(e.Timestamp))
	binary.BigEndian.PutUint32(buf[8:], keyLen)
	copy(buf[12:], keyBytes)
	binary.BigEndian.PutUint32(buf[12+keyLen:], valLen)
	copy(buf[16+keyLen:], e.Value)

	return buf
}

func Decode(data []byte) (*WALEntry, error) {
	if len(data) < 16 {
		return nil, errors.New("data too short to contain header")
	}

	ts := int64(binary.BigEndian.Uint64(data[0:8]))
	keyLen := binary.BigEndian.Uint32(data[8:12])

	keyStart := 12
	keyEnd := keyStart + int(keyLen)

	if len(data) < keyEnd+4 {
		return nil, errors.New("data too short to contain key + value length")
	}

	key := string(data[keyStart:keyEnd])
	valLen := binary.BigEndian.Uint32(data[keyEnd : keyEnd+4])

	valStart := keyEnd + 4
	valEnd := valStart + int(valLen)

	if len(data) < valEnd {
		return nil, errors.New("data too short to contain full value")
	}

	value := make([]byte, valLen)
	copy(value, data[valStart:valEnd])

	return &WALEntry{
		Timestamp: ts,
		Key:       key,
		Value:     value,
	}, nil
}
