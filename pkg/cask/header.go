package cask

import (
	"encoding/binary"
	"io"
)

var (
	byteOrder  binary.ByteOrder = binary.LittleEndian
	headerSize uint32           = 12
)

type header struct {
	Timestamp, KeySize, ValueSize uint32
}

func newHeader(t, ksz, vsz uint32) header {
	return header{
		Timestamp: t,
		KeySize:   ksz,
		ValueSize: vsz,
	}
}

func (h header) encode() []byte {
	b := make([]byte, headerSize)

	byteOrder.PutUint32(b[0:4], h.Timestamp)
	byteOrder.PutUint32(b[4:8], h.KeySize)
	byteOrder.PutUint32(b[8:], h.ValueSize)

	return b
}

func (h header) entrySize() uint32 {
	return headerSize + h.KeySize + h.ValueSize
}

func (h header) isTombstone() bool {
	return h.KeySize == 0
}

func parseHeader(r io.Reader) (header, error) {
	h := header{}

	return h, binary.Read(r, byteOrder, &h)
}
