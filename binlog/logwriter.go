package binlog

import (
	"bufio"
	"os"
	"bytes"
	"log"
)


func NewEventSet(filename string) *EventLogSet{
	log:=NewRWlog(filename)
	return &EventLogSet{log}
}

func (eventLogSet *EventLogSet) WriteEvent(event Event) bool{
	bufferdWriter:=bufio.NewWriter(eventLogSet.bLog.file)
	filesize, _ := eventLogSet.bLog.file.Seek(0, os.SEEK_END)
	var head bytes.Buffer
	head.Write(Int32ToBytes(event.EventHeader.Crc32))
	head.WriteByte(event.EventHeader.Compressed)
	head.Write(Int64ToBytes(filesize))
	head.Write(Int32ToBytes(event.EventHeader.Length))
	head.Write(UnitToBytes(event.EventHeader.Flags))
	bufferdWriter.Write(head.Bytes())
	bufferdWriter.Write(event.EventData)
	err:=bufferdWriter.Flush()
	if err!=nil{
		log.Fatalln(err)
		return false
	}
	return true
}