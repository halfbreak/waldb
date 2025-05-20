package wal

import (
	"fmt"
	"io"
	"os"
	"strconv"
)

type WALMetadata struct {
	Index int
	file  *os.File
}

func OpenMetadata(path string) (*WALMetadata, error) {
	_, err := os.Stat(fmt.Sprintf("%s/wal.meta", path))
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
	return &WALMetadata{
		Index: index,
		file:  f,
	}, nil
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

func (metadata *WALMetadata) WriteMetadata() error {
	_, err := metadata.file.WriteString(fmt.Sprintf("%d", metadata.Index))
	return err
}

func (metadata *WALMetadata) Close() {
	metadata.file.Sync()
	metadata.file.Close()
}
