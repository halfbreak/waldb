package main

import (
	"errors"
	"sync"

	"halfbreak/waldb/wal"

	"github.com/magiconair/properties"
)

type KeyValueStore struct {
	wal   *wal.WAL
	cache *map[string][]byte
	mu    sync.Mutex
}

func NewKVS() (*KeyValueStore, error) {
	props, err := loadProperties("./metadata.properties")
	if err != nil {
		return nil, err
	}
	wal, err := wal.NewWAL(props["wal.file.location"])
	if err != nil {
		return nil, err
	}
	return &KeyValueStore{
		wal:   wal,
		cache: &map[string][]byte{},
	}, nil
}

func (kv *KeyValueStore) Add(key string, value []byte) error {
	err := kv.wal.Append(key, value)
	if err != nil {
		return err
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	(*kv.cache)[key] = value
	return nil
}

func (kv *KeyValueStore) Read(key string) ([]byte, error) {
	val, ok := (*kv.cache)[key]
	if !ok {
		return nil, errors.New("not found")
	}
	return val, nil
}

func loadProperties(path string) (map[string]string, error) {
	p, err := properties.LoadFile(path, properties.UTF8)
	if err != nil {
		return nil, err
	}

	props := make(map[string]string)
	for _, key := range p.Keys() {
		val, _ := p.Get(key)
		props[key] = val
	}

	return props, nil
}
