package wal

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"sync"
	"time"
)

const flushInterval = 10 * time.Millisecond

type WAL struct {
	mu         sync.Mutex
	segment    *WalSegment
	index      int
	wg         sync.WaitGroup
	folder     string
	entries    chan *WALEntry
	metaFile   *os.File
	metaWriter *bufio.Writer
}

func NewWAL(path string) (*WAL, error) {
	segment, err := NewWalSegment(path, 0)
	if err != nil {
		return nil, err
	}

	_, err = os.Stat(fmt.Sprintf("%s/wal.meta", path))
	fileDoesNotExist := err != nil && os.IsNotExist(err)

	var index int
	if fileDoesNotExist {
		index = 0
	} else {
		i, err := fetchIndex(path)
		if err != nil {
			panic(err)
		}
		index = i
	}

	f, err := os.OpenFile(fmt.Sprintf("%s/wal.meta", path), os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0644)
	if err != nil {
		return nil, err
	}

	wal := &WAL{
		segment:    segment,
		index:      index,
		folder:     path,
		entries:    make(chan *WALEntry, 1000),
		metaFile:   f,
		metaWriter: bufio.NewWriterSize(f, 4*1024), // 4KB buffer
	}

	err = wal.writeMetadata()
	if err != nil {
		panic(err)
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

	wal.index = wal.index + 1
	newSegment, err := NewWalSegment(wal.folder, wal.index)
	if err != nil {
		panic(err)
	}
	wal.segment = newSegment
	err = wal.writeMetadata()
	if err != nil {
		panic(err)
	}
}

func (wal *WAL) writeMetadata() error {
	_, err := wal.metaFile.WriteString(fmt.Sprintf("%d", wal.index))
	return err
}

func fetchIndex(path string) (int, error) {
	f, err := os.OpenFile(fmt.Sprintf("%s/wal.meta", path), os.O_RDONLY, 0644)
	if err != nil {
		return 0, err
	}

	data, err := io.ReadAll(f)
	if err != nil {
		return 0, err
	}

	i, err := strconv.Atoi(string(data))
	if err != nil {
		panic(err)
	}
	return i, nil
}

func (wal *WAL) Close() error {
	close(wal.entries)
	wal.wg.Wait()

	wal.metaFile.Sync()
	wal.metaFile.Close()

	return wal.segment.Close()
}
