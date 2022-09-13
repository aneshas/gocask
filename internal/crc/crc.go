package crc

import "hash/crc32"

const ieee = 0xedb88320

// CalcCRC32 calculates CalcCRC32 for the value
func CalcCRC32(val []byte) uint32 {
	return crc32.Checksum(val, crc32.MakeTable(ieee))
}
