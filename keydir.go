package gocask

import (
	"fmt"
	"sync"
)

type kdEntry struct {
	ValuePos  int64
	ValueSize int32
	Timestamp int32
	File      string
}

func newKeydir() *keydir {
	return &keydir{
		entries: map[string]kdEntry{},
	}
}

type keydir struct {
	m          sync.RWMutex
	entries    map[string]kdEntry
	lastOffset int64
}

func (kd *keydir) Set(key string, h header, file string) {
	kd.m.Lock()
	defer kd.m.Unlock()

	entry := kdEntry{
		ValuePos:  kd.lastOffset + h.EntrySize() - int64(h.Vsz),
		ValueSize: h.Vsz,
		Timestamp: h.T,
		File:      file,
	}

	kd.lastOffset = kd.lastOffset + h.EntrySize()

	kd.entries[key] = entry
}

func (kd *keydir) Get(key string) (kdEntry, error) {
	kd.m.RLock()
	defer kd.m.RUnlock()

	ke, ok := kd.entries[key]
	if !ok {
		return kdEntry{}, fmt.Errorf("no value found for key: %s", key)
	}

	return ke, nil
}
