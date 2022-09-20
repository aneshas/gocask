package core

type kdEntry struct {
	CRC       uint32
	Timestamp uint32
	ValuePos  uint32
	ValueSize uint32
	File      string
}

type keyDir struct {
	lastOffset uint32
	entries    map[string]kdEntry
}

func newKeyDir() *keyDir {
	return &keyDir{
		entries: map[string]kdEntry{},
	}
}

func (kd *keyDir) set(key string, h header, file string) {
	entry := kdEntry{
		CRC:       h.CRC,
		ValuePos:  kd.lastOffset + h.entrySize() - h.ValueSize,
		ValueSize: h.ValueSize,
		Timestamp: h.Timestamp,
		File:      file,
	}

	kd.lastOffset = kd.lastOffset + h.entrySize()

	kd.entries[key] = entry
}

func (kd *keyDir) get(key string) (kdEntry, error) {
	ke, ok := kd.entries[key]
	if !ok {
		return kdEntry{}, ErrKeyNotFound
	}

	return ke, nil
}

func (kd *keyDir) unset(key string) {
	delete(kd.entries, key)

	kd.lastOffset = kd.lastOffset + headerSize + uint32(len(key))
}

func (kd *keyDir) resetOffset() {
	kd.lastOffset = 0
}

func (kd *keyDir) advanceOffsetBy(n uint32) {
	kd.lastOffset += n
}

func (kd *keyDir) keys() []string {
	keys := []string{}

	for key := range kd.entries {
		keys = append(keys, key)
	}

	return keys
}
