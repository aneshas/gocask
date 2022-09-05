package cask

import (
	"bytes"
	"encoding/binary"
)

var byteOrder binary.ByteOrder = binary.LittleEndian

func newHeader(t, ksz, vsz uint32) header {
	return header{
		Timestamp: t,
		KeySize:   ksz,
		ValueSize: vsz,
	}
}

type header struct {
	Timestamp, KeySize, ValueSize uint32
}

func (h header) Encode() []byte {
	b := make([]byte, 12)

	byteOrder.PutUint32(b[0:4], h.Timestamp)
	byteOrder.PutUint32(b[4:8], h.KeySize)
	byteOrder.PutUint32(b[8:], h.ValueSize)

	return b
}

func (h header) EntrySize() uint32 {
	return 12 + h.KeySize + h.ValueSize
}

func parseHeader(file File) (header, error) {
	h := header{}

	hb := make([]byte, 12)

	_, err := file.Read(hb)
	if err != nil {
		return h, err
	}

	r := bytes.NewReader(hb)

	err = binary.Read(r, byteOrder, &h.Timestamp)
	if err != nil {
		return h, err
	}

	err = binary.Read(r, byteOrder, &h.KeySize)
	if err != nil {
		return h, err
	}

	err = binary.Read(r, byteOrder, &h.ValueSize)
	if err != nil {
		return h, err
	}

	return h, nil
}
