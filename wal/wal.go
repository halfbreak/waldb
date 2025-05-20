package wal

import (
	"log"
	"sync"
	"time"
)

const flushInterval = 10 * time.Millisecond

type WAL struct {
	mu       sync.Mutex
	segment  *WalSegment
	wg       sync.WaitGroup
	folder   string
	entries  chan *WALEntry
	metadata *WALMetadata
}

func NewWAL(path string) (*WAL, error) {
	segment, err := NewWalSegment(path, 0)
	if err != nil {
		return nil, err
	}

	metadata, err := OpenMetadata(path)
	if err != nil {
		return nil, err
	}

	wal := &WAL{
		segment:  segment,
		folder:   path,
		entries:  make(chan *WALEntry, 1000),
		metadata: metadata,
	}

	err = wal.metadata.WriteMetadata()
	if err != nil {
		return nil, err
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

	flushTicker := time.NewTicker(flushInterval)
	defer flushTicker.Stop()

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

		case <-flushTicker.C:
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

		if wal.segment.IsFull(data) {
			wal.rollToNewSegment()
		}

		wal.segment.Append(data)
	}

	if err := wal.segment.Flush(); err != nil {
		panic(err)
	}

	for _, entry := range batch {
		entry.done <- nil
	}
}

func (wal *WAL) rollToNewSegment() {
	log.Println("Rolling to a new segment.")

	err := wal.segment.Close()
	if err != nil {
		panic(err)
	}

	wal.metadata.Index = wal.metadata.Index + 1
	newSegment, err := NewWalSegment(wal.folder, wal.metadata.Index)
	if err != nil {
		panic(err)
	}
	wal.segment = newSegment
	err = wal.metadata.WriteMetadata()
	if err != nil {
		panic(err)
	}
}

func (wal *WAL) Close() error {
	close(wal.entries)
	wal.wg.Wait()
	wal.metadata.Close()

	return wal.segment.Close()
}
