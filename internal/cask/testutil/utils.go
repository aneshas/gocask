package testutil

import "encoding/binary"

var bo binary.ByteOrder = binary.LittleEndian

func Entry(now uint32, key, val []byte) []byte {
	return AppendBytes(
		U32ToB(now),
		U32ToB(uint32(len(key))),
		U32ToB(uint32(len(val))),
		key,
		val,
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
