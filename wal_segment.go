package main

import (
	"fmt"
	"github.com/edsrzf/mmap-go"
	"os"
)

const mmapSize = 1 << 20 // 100MB

type WalSegment struct {
	file        *os.File
	mmappedData *mmap.MMap
	offset      int
}

func NewWalSegment(folder string, index int) (*WalSegment, error) {
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
		return nil, err
	}

	segment := &WalSegment{
		file:        f,
		mmappedData: &mmappedData,
		offset:      0,
	}
	return segment, nil
}

func (segment *WalSegment) Append(data []byte) {
	copy((*segment.mmappedData)[segment.offset:], data)
	segment.offset += len(data)
}

func (segment *WalSegment) IsFull(data []byte) bool {
	return segment.offset+len(data) > mmapSize
}

func (segment *WalSegment) Flush() error {
	return (segment.mmappedData).Flush()
}

func (segment *WalSegment) Close() error {
	if err := (segment.mmappedData).Flush(); err != nil {
		return err
	}
	if err := segment.mmappedData.Unmap(); err != nil {
		return err
	}
	return segment.file.Close()
}
