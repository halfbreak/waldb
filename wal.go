package main

import (
	"encoding/binary"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/edsrzf/mmap-go"
)

const mmapSize = 1 << 20 // 100MB

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
	segment *WalSegment
	index   int
	wg      sync.WaitGroup
	folder  string
	entries chan *WALEntry
}

type WalSegment struct {
	file        *os.File
	mmappedData *mmap.MMap
	offset      int
}

func NewWAL(path string) (*WAL, error) {
	segment, err := newWalSegment(path, 0)
	if err != nil {
		panic(err)
	}

	wal := &WAL{
		segment: segment,
		index:   0,
		folder:  path,
		entries: make(chan *WALEntry, 1000),
	}

	wal.wg.Add(1)
	go wal.writeLoop()

	return wal, nil
}

func newWalSegment(folder string, index int) (*WalSegment, error) {
	fileName := fmt.Sprintf("%s/wal_%d.db", folder, index)
	f, err := os.OpenFile(fileName, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	if err := f.Truncate(int64(mmapSize)); err != nil {
		panic(err)
	}

	mmappedData, err := mmap.Map(f, mmap.RDWR, 0)
	if err != nil {
		panic(err)
	}

	segment := &WalSegment{
		file:        f,
		mmappedData: &mmappedData,
		offset:      0,
	}
	return segment, nil
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
			batch = append(batch, entry)
			if len(batch) >= 100 {
				wal.flushBatch(batch)
				batch = batch[:0]
			}

		case <-flushInterval.C:
			if len(batch) > 0 {
				wal.flushBatch(batch)
				batch = batch[:0]
			}
		}
	}
}

func (wal *WAL) flushBatch(batch []*WALEntry) {
	wal.mu.Lock()
	defer wal.mu.Unlock()

	for _, entry := range batch {
		data := entry.Encode()

		if wal.segment.offset+len(data) > mmapSize {
			log.Println("Rolling to a new file.")

			if err := (*wal.segment.mmappedData).Flush(); err != nil {
				panic(err)
			}
			if err := wal.segment.mmappedData.Unmap(); err != nil {
				panic(err)
			}

			wal.index = wal.index + 1
			newSegment, err := newWalSegment(wal.folder, wal.index)
			if err != nil {
				panic(err)
			}
			wal.segment = newSegment
		}

		copy((*wal.segment.mmappedData)[wal.segment.offset:], data)
		wal.segment.offset += len(data)
	}

	if err := (*wal.segment.mmappedData).Flush(); err != nil {
		panic(err)
	}

	for _, entry := range batch {
		entry.done <- nil
	}
}

func (wal *WAL) Close() error {
	close(wal.entries)
	wal.wg.Wait()

	if err := (*wal.segment.mmappedData).Flush(); err != nil {
		panic(err)
	}
	if err := wal.segment.mmappedData.Unmap(); err != nil {
		panic(err)
	}
	return wal.segment.file.Close()
}
