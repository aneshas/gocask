package core

type kdEntry struct {
	h        header
	ValuePos uint32
	File     string
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

func (kd *keyDir) mapEntries(file string, f func([]byte, *kdEntry) error) error {
	var err error

	for key, entry := range kd.entries {
		if entry.File != file {
			continue
		}

		err = f([]byte(key), &entry)
		if err != nil {
			return err
		}
	}

	return nil
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

func (kd *keyDir) setFromHint(key []byte, h hintHeader, file string) *kdEntry {
	entry := kdEntry{
		h:        h.header,
		ValuePos: h.ValuePos,
		File:     file,
	}

	kd.entries[string(key)] = entry

	return &entry
}

func (kd *keyDir) get(key []byte) (kdEntry, error) {
	ke, ok := kd.entries[string(key)]
	if !ok {
		return kdEntry{}, ErrKeyNotFound
	}

	return ke, nil
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
		newEntry := kd.entries[key]

		newEntry.File = entry.File
		newEntry.ValuePos = entry.ValuePos

		kd.entries[key] = newEntry
	}
}
