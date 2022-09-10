package cask

import (
	"sync"
)

type kdEntry struct {
	Timestamp uint32
	ValuePos  uint32
	ValueSize uint32
	File      string
}

type keyDir struct {
	lastOffset uint32
	m          sync.RWMutex
	entries    map[string]kdEntry
}

func newKeyDir() *keyDir {
	return &keyDir{
		entries: map[string]kdEntry{},
	}
}

func (kd *keyDir) set(key string, h header, file string) {
	kd.m.Lock()
	defer kd.m.Unlock()

	entry := kdEntry{
		ValuePos:  kd.lastOffset + h.entrySize() - h.ValueSize,
		ValueSize: h.ValueSize,
		Timestamp: h.Timestamp,
		File:      file,
	}

	kd.lastOffset = kd.lastOffset + h.entrySize()

	kd.entries[key] = entry
}

func (kd *keyDir) get(key string) (kdEntry, error) {
	kd.m.RLock()
	defer kd.m.RUnlock()

	ke, ok := kd.entries[key]
	if !ok {
		return kdEntry{}, ErrKeyNotFound
	}

	return ke, nil
}

func (kd *keyDir) unset(key string) {
	kd.m.Lock()
	defer kd.m.Unlock()

	delete(kd.entries, key)

	kd.lastOffset = kd.lastOffset + headerSize + uint32(len(key))
}

func (kd *keyDir) resetOffset() {
	kd.lastOffset = 0
}

func (kd *keyDir) keys() []string {
	kd.m.RLock()
	defer kd.m.RUnlock()

	keys := []string{}

	for key := range kd.entries {
		keys = append(keys, key)
	}

	return keys
}
