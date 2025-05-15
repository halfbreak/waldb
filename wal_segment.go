package main

import (
	"fmt"
	"github.com/edsrzf/mmap-go"
	"os"
)

type WalSegment struct {
	file        *os.File
	mmappedData *mmap.MMap
	offset      int
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
		return nil, err
	}

	segment := &WalSegment{
		file:        f,
		mmappedData: &mmappedData,
		offset:      0,
	}
	return segment, nil
}
