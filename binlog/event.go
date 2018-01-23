package binlog


type EventHeader struct {
	Crc32 int32
	Compressed byte
	Offset int64
	Length int32
	Flags uint16
}


type Event struct {
	EventHeader EventHeader
	EventData []byte
}

