package gocask

import (
	"encoding/binary"
	"os"
)

func newHeader(t, ksz, vsz int32) header {
	return header{
		T:   t,
		Ksz: ksz,
		Vsz: vsz,
	}
}

// TODO We probably need to work with unsigned integers
type header struct {
	T, Ksz, Vsz int32
}

func (h header) Encode() []byte {
	b := make([]byte, 12)

	binary.LittleEndian.PutUint32(b[0:4], uint32(h.T))
	binary.LittleEndian.PutUint32(b[4:8], uint32(h.Ksz))
	binary.LittleEndian.PutUint32(b[8:], uint32(h.Vsz))

	return b
}

func (h header) EntrySize() int64 {
	return int64(12 + h.Ksz + h.Vsz)
}

func parseHeader(file *os.File) (header, error) {
	h := header{}

	err := binary.Read(file, binary.LittleEndian, &h.T)
	if err != nil {
		return h, err
	}

	err = binary.Read(file, binary.LittleEndian, &h.Ksz)
	if err != nil {
		return h, err
	}

	err = binary.Read(file, binary.LittleEndian, &h.Vsz)
	if err != nil {
		return h, err
	}

	return h, nil
}
