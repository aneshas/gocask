package cask

import (
	"fmt"
	"sync"
)

type kdEntry struct {
	Timestamp uint32
	ValuePos  uint32
	ValueSize uint32
	File      string
}

func newKeyDir() *keyDir {
	return &keyDir{
		entries: map[string]kdEntry{},
	}
}

type keyDir struct {
	lastOffset uint32
	m          sync.RWMutex
	entries    map[string]kdEntry
}

func (kd *keyDir) Set(key string, h header, file string) {
	kd.m.Lock()
	defer kd.m.Unlock()

	entry := kdEntry{
		ValuePos:  kd.lastOffset + h.EntrySize() - h.ValueSize,
		ValueSize: h.ValueSize,
		Timestamp: h.Timestamp,
		File:      file,
	}

	kd.lastOffset = kd.lastOffset + h.EntrySize()

	kd.entries[key] = entry
}

func (kd *keyDir) Get(key string) (kdEntry, error) {
	kd.m.RLock()
	defer kd.m.RUnlock()

	ke, ok := kd.entries[key]
	if !ok {
		return kdEntry{}, fmt.Errorf("no value found for key: %s", key)
	}

	return ke, nil
}
