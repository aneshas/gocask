package core

import (
	"bufio"
	"io"
)

func parseHintEntry(r *bufio.Reader) (hintEntry, error) {
	h, err := parseHintHeader(r)
	if err != nil {
		return hintEntry{}, err
	}

	key := make([]byte, h.KeySize)

	_, err = io.ReadFull(r, key)

	return newHintEntry(h, key), err
}

func newHintEntry(h hintHeader, key []byte) hintEntry {
	return hintEntry{
		hintHeader: h,
		key:        key,
	}
}

type hintEntry struct {
	hintHeader
	key []byte
}

func (he hintEntry) serialize() []byte {
	b := make([]byte, 0, int(hintHeaderSize)+len(he.key))

	// reuse buffer for encoding (check / profile for other such optimisations)
	return append(
		append(b, he.hintHeader.serialize()...),
		he.key...,
	)
}

func parseKVEntry(r *bufio.Reader) (kvEntry, error) {
	ke, err := parseKEntry(r)
	if err != nil {
		return kvEntry{}, err
	}

	if ke.isTombstone() {
		return kvEntry{isTombstone: true}, nil
	}

	val := make([]byte, ke.ValueSize)

	_, err = io.ReadFull(r, val)
	if err != nil {
		return kvEntry{}, err
	}

	// TODO - Check CRC

	return kvEntry{
		kEntry: ke,
		val:    val,
	}, nil
}

func newKVEntry(t uint32, key, val []byte) kvEntry {
	return kvEntry{
		kEntry: kEntry{
			header: newKVHeader(t, key, val),
			key:    key,
		},
		val: val,
	}
}

// TODO - Reuse these where possible
type kvEntry struct {
	kEntry
	isTombstone bool
	val         []byte
}

func (kv *kvEntry) serialize() []byte {
	b := make([]byte, 0, int(headerSize)+len(kv.key)+len(kv.val))

	// reuse buffer for encoding (check / profile for other such optimisations)
	b = append(b, kv.header.serialize()...)

	if kv.key != nil {
		b = append(b, kv.key...)
	}

	return append(b, kv.val...)
}

func parseKEntry(r *bufio.Reader) (kEntry, error) {
	h, err := parseHeader(r)
	if err != nil {
		return kEntry{}, err
	}

	keySize := h.KeySize

	if h.isTombstone() {
		keySize = h.ValueSize
	}

	key := make([]byte, keySize)

	_, err = io.ReadFull(r, key)
	if err != nil {
		return kEntry{}, err
	}

	return kEntry{
		header: h,
		key:    key,
	}, nil
}

type kEntry struct {
	header
	key []byte
}
