package bloom

import (
	"encoding/binary"
)

func uint16ToByte(i uint16) []byte {
	data := make([]byte, 2)
	binary.BigEndian.PutUint16(data, i)
	return data
}

func uint32ToByte(i uint32) []byte {
	data := make([]byte, 4)
	binary.BigEndian.PutUint32(data, i)
	return data
}

func uint64ToByte(i uint64) []byte {
	data := make([]byte, 8)
	binary.BigEndian.PutUint64(data, i)
	return data
}
