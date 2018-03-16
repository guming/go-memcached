package binlog

import (
	"bufio"
	"bytes"
	"log"
	"io"
	"strings"
)



func NewEventSet(dir string) *EventLogSet{
	_,logfilepath:=showMaxfileIdx(dir)
	if strings.EqualFold(logfilepath,""){
		logfilepath=dir+"/000001.bin"
	}
	log.Println(logfilepath,dir)
	log:=NewRWlog(logfilepath)
	return &EventLogSet{log}
}

func (eventLogSet *EventLogSet) WriteEvent(event Event) bool{
	filesize, _ := eventLogSet.BLog.file.Seek(0, io.SeekEnd)
	var canWrite=true
	if filesize>=LOG_MAX_SIZE{
		canWrite=eventLogSet.BLog.Rolling()
	}
	if canWrite==true {
		bufferdWriter := bufio.NewWriter(eventLogSet.BLog.file)
		var head bytes.Buffer
		head.Write(Int32ToBytes(event.EventHeader.Crc32))
		head.WriteByte(event.EventHeader.Compressed)
		head.Write(Int64ToBytes(filesize))
		head.Write(Int32ToBytes(event.EventHeader.Length))
		head.Write(UnitToBytes(event.EventHeader.Flags))
		bufferdWriter.Write(head.Bytes())
		bufferdWriter.Write(event.EventData)
		err := bufferdWriter.Flush()
		if err != nil {
			log.Fatalln(err)
			return false
		}
		return true
	}else{
		log.Fatal("cannot flushed to file:",eventLogSet.BLog.Filename)
		return false
	}
}