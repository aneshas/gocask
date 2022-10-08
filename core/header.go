package core

import (
	"encoding/binary"
	"github.com/aneshas/gocask/internal/crc"
	"io"
)

var (
	byteOrder      binary.ByteOrder = binary.LittleEndian
	headerSize     uint32           = 16
	hintHeaderSize uint32           = 20
)

type header struct {
	CRC, Timestamp, KeySize, ValueSize uint32
}

func (h header) toHint(vpos uint32) hintHeader {
	var hh hintHeader

	hh.header = h
	hh.ValuePos = vpos

	return hh
}

func newKVHeader(t uint32, key, val []byte) header {
	var kSize uint32

	if key != nil {
		kSize = uint32(len(key))
	}

	vSize := uint32(len(val))

	return newHeader(crc.CalcCRC32(val), t, kSize, vSize)
}

func newHeader(crc, t, ksz, vsz uint32) header {
	return header{
		CRC:       crc,
		Timestamp: t,
		KeySize:   ksz,
		ValueSize: vsz,
	}
}

func (h header) serialize() []byte {
	b := make([]byte, headerSize)

	byteOrder.PutUint32(b[0:4], h.CRC)
	byteOrder.PutUint32(b[4:8], h.Timestamp)
	byteOrder.PutUint32(b[8:12], h.KeySize)
	byteOrder.PutUint32(b[12:], h.ValueSize)

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

type hintHeader struct {
	header
	ValuePos uint32
}

func (h hintHeader) serialize() []byte {
	b := make([]byte, hintHeaderSize)

	byteOrder.PutUint32(b[0:4], h.CRC)
	byteOrder.PutUint32(b[4:8], h.Timestamp)
	byteOrder.PutUint32(b[8:12], h.KeySize)
	byteOrder.PutUint32(b[12:16], h.ValueSize)
	byteOrder.PutUint32(b[16:], h.ValuePos)

	return b
}

func parseHintHeader(r io.Reader) (hintHeader, error) {
	h := hintHeader{}

	return h, binary.Read(r, byteOrder, &h)
}
