package main

import (
	"bufio"
	"encoding/binary"
	"os"
	"sync"
	"time"
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

type WAL struct {
	mu      sync.Mutex
	file    *os.File
	writer  *bufio.Writer
	entries chan *WALEntry
	wg      sync.WaitGroup
}

func NewWAL(path string) (*WAL, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}

	wal := &WAL{
		file:    f,
		writer:  bufio.NewWriterSize(f, 4*1024), // 4KB buffer
		entries: make(chan *WALEntry, 1000),
	}

	wal.wg.Add(1)
	go wal.writeLoop()

	return wal, nil
}

func (wal *WAL) Append(key string, value []byte) error {
	done := make(chan error, 1)
	entry := &WALEntry{
		Timestamp: time.Now().UnixNano(),
		Key:       key,
		Value:     value,
		done:      done,
	}

	wal.entries <- entry
	return <-done
}

func (wal *WAL) writeLoop() {
	defer wal.wg.Done()

	batch := make([]*WALEntry, 0, 100)

	flushInterval := time.NewTicker(10 * time.Millisecond)
	defer flushInterval.Stop()

	for {
		select {
		case entry, ok := <-wal.entries:
			if !ok {
				wal.flushBatch(batch)
				return
			}

			//log.Println("Adding entries to batch")
			batch = append(batch, entry)
			if len(batch) >= 100 {
				wal.flushBatch(batch)
				batch = batch[:0]
			}

		case <-flushInterval.C:
			//log.Println("Triggered timer")
			if len(batch) > 0 {
				wal.flushBatch(batch)
				batch = batch[:0]
			}
		}
	}
}

func (wal *WAL) flushBatch(batch []*WALEntry) {
	//log.Println("Flushing")
	wal.mu.Lock()
	defer wal.mu.Unlock()

	for _, entry := range batch {
		data := entry.Encode()
		wal.writer.Write(data)
	}
	wal.writer.Flush()
	wal.file.Sync()

	for _, entry := range batch {
		entry.done <- nil
	}
}

func (wal *WAL) Close() error {
	close(wal.entries)
	wal.wg.Wait()

	wal.writer.Flush()
	wal.file.Sync()
	return wal.file.Close()
}
