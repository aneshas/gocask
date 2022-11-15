package testutil

import (
	"encoding/binary"
	"github.com/aneshas/gocask/internal/crc"
)

var bo binary.ByteOrder = binary.LittleEndian

func Entry(now uint32, key, val []byte) []byte {
	return AppendBytes(
		U32ToB(crc.CalcCRC32(val)),
		U32ToB(now),
		U32ToB(uint32(len(key))),
		U32ToB(uint32(len(val))),
		key,
		val,
	)
}

func HintEntry(now uint32, key, val []byte, vPos int) []byte {
	vp := uint32(vPos + 16 + len(key))

	return AppendBytes(
		U32ToB(crc.CalcCRC32(val)),
		U32ToB(now),
		U32ToB(uint32(len(key))),
		U32ToB(uint32(len(val))),
		U32ToB(vp),
		key,
	)
}

func U32ToB(i uint32) []byte {
	b := make([]byte, 4)

	bo.PutUint32(b, i)

	return b
}

func AppendBytes(chunks ...[]byte) []byte {
	var b []byte

	for _, c := range chunks {
		b = append(b, c...)
	}

	return b
}
