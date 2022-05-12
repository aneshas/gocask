package gocask

import (
	"encoding/binary"
)

func newHeader(t, ksz, vsz int32) header {
	h := make(header, 12)

	binary.LittleEndian.PutUint32(h[0:4], uint32(t))
	binary.LittleEndian.PutUint32(h[4:8], uint32(ksz))
	binary.LittleEndian.PutUint32(h[8:], uint32(vsz))

	return h
}

func emptyHeader() header {
	return make(header, 12)
}

type header []byte

func (h header) Parse() (int32, int32, int32, error) {
	t := binary.LittleEndian.Uint32(h[:4])
	ksz := binary.LittleEndian.Uint32(h[4:8])
	vsz := binary.LittleEndian.Uint32(h[8:])

	return int32(t), int32(ksz), int32(vsz), nil
}
