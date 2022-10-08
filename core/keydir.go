package core

type kdEntry struct {
	h        header
	ValuePos uint32
	File     string
}

func (ke kdEntry) hintHeader() hintHeader {
	return ke.h.toHint(ke.ValuePos)
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

func (kd *keyDir) set(key []byte, h header, file string) *kdEntry {
	entry := kdEntry{
		h:        h,
		ValuePos: kd.lastOffset + h.entrySize() - h.ValueSize,
		File:     file,
	}

	kd.lastOffset = kd.lastOffset + h.entrySize()

	kd.entries[string(key)] = entry

	return &entry
}

func (kd *keyDir) setFromHint(h hintEntry, file string) *kdEntry {
	entry := kdEntry{
		h:        h.header,
		ValuePos: h.ValuePos,
		File:     file,
	}

	kd.entries[string(h.key)] = entry

	return &entry
}

func (kd *keyDir) get(key []byte) (*kdEntry, error) {
	ke, ok := kd.entries[string(key)]
	if !ok {
		return nil, ErrKeyNotFound
	}

	return &ke, nil
}

func (kd *keyDir) unset(key []byte) {
	delete(kd.entries, string(key))

	kd.lastOffset = kd.lastOffset + headerSize + uint32(len(key))
}

func (kd *keyDir) resetOffset() {
	kd.lastOffset = 0
}

func (kd *keyDir) advanceOffsetBy(n uint32) {
	kd.lastOffset += n
}

func (kd *keyDir) keys() []string {
	// This duplicates all keys allocates a lot of memory potentially exhausting it - does it make sense?
	// Stream values instead?

	keys := []string{}

	for key := range kd.entries {
		keys = append(keys, key)
	}

	return keys
}

func (kd *keyDir) merge(other *keyDir) {
	for key, entry := range other.entries {
		e, ok := kd.entries[key]
		if !ok {
			// this accounts for the case when entry has been deleted
			// in a future data file (in that case it won't be present in the keydir)
			continue
		}

		e.File = entry.File
		e.ValuePos = entry.ValuePos

		kd.entries[key] = e
	}
}
