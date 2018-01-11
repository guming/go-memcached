package main

import (
	"encoding/binary"
)

func BytesToString(buf []byte) string {
	return string(buf)
}

func BytesToInt32(buf []byte) int32 {
	return int32(binary.BigEndian.Uint32(buf))
}

func BytesToInt64(buf []byte) int64 {
	return int64(binary.BigEndian.Uint64(buf))
}

func BytesToUint64(buf []byte) uint64 {
	return uint64(binary.BigEndian.Uint64(buf))
}

func BytesToUint32(buf []byte) uint32 {
	return uint32(binary.BigEndian.Uint32(buf))
}

func Int32ToBytes(i int32) []byte {
	var buf = make([]byte, 4)
	binary.BigEndian.PutUint32(buf,uint32(i))
	return buf
}

func Int64ToBytes(i int64) []byte {
	var buf = make([]byte, 8)
	binary.BigEndian.PutUint64(buf,uint64(i))
	return buf
}

func Unit32ToBytes(i uint32) []byte {
	var buf = make([]byte, 4)
	binary.BigEndian.PutUint32(buf,i)
	return buf
}