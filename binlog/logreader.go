package binlog

import (
	"bufio"
	"fmt"
	"container/list"
	"bytes"
	"log"
)

func LoadEventSet(dir string) *EventLogSet{
	maxfileidx,logfilepath:=showMaxfileIdx(dir)
	//todo
	if maxfileidx>0 {
		maxfileidx = maxfileidx - 1
	}
	startsize:=LOG_MAX_SIZE*(maxfileidx)

	l:=LoadReadlog(logfilepath,startsize)
	log.Println("load file:",logfilepath,startsize)
	return &EventLogSet{l}
}

func (eventLogSet *EventLogSet) ReadEvents(offset int64,fetch int64) (*list.List, int64){
	max:=eventLogSet.BLog.Seek()
	events := list.New()
	if offset>=max{
		return events,int64(-1)
	}
	if offset==0{
		offset+=8
	}
	rbytes := make([]byte, fetch)
	bufferdReader := bufio.NewReader(eventLogSet.BLog.file)
	eventLogSet.BLog.SeekReadOffset(rbytes, bufferdReader, offset)
	readcount:=int64(0)
	fmt.Printf("read count %d %d\n",readcount,offset)
	for  {
		if fetch-readcount>19 && readcount+offset<max{
			fmt.Printf("header is %d %s %d %d %d\n", BytesToInt32(rbytes[readcount:readcount + 4]),
				string(rbytes[readcount + 4:readcount + 5]), BytesToInt64(rbytes[readcount + 5:readcount + 13]),
				BytesToInt32(rbytes[readcount + 13:readcount + 17]), BytesToUint16(rbytes[readcount + 17:readcount + 19]))
			lens := BytesToInt32(rbytes[readcount + 13:readcount + 17])
			if int64(lens) + readcount > fetch {
				break
			}
			values := rbytes[readcount + 19:readcount + 19 + int64(lens)]
			fmt.Println("value is %s\n", BytesToString(values))
			event := Event{EventHeader{Crc32:BytesToInt32(rbytes[readcount:readcount + 4])}, values}
			events.PushBack(event)
			readcount += 19 + int64(lens)
			fmt.Printf("read count %d %d\n",readcount,offset)
		}
	}
	return events,readcount+offset
}

func (eventLogSet *EventLogSet) ReadEvent(offset int64) (Event,int64){
	position:=eventLogSet.BLog.Seek()
	if offset>=position{
		return Event{},int64(-1)
	}
	bufferdReader:=bufio.NewReader(eventLogSet.BLog.file)
	if offset==0{
		offset+=8
	}
	buf:=make([]byte,19)
	fmt.Printf("offset is %d\n",offset)
	eventLogSet.BLog.SeekReadOffset(buf,bufferdReader,offset)
	fmt.Printf("header is %d %s %d %d %d\n", BytesToInt32(buf[0:4]),
		string(buf[4:5]),BytesToInt64(buf[5:13]),
		BytesToInt32(buf[13:17]),BytesToUint16(buf[17:]))
	lens:=BytesToInt32(buf[13:17])
	fmt.Printf("lens is %d\n",lens)
	values:=make([]byte,lens)
	eventLogSet.BLog.SeekReadOffset(values,bufferdReader,offset+19)
	fmt.Printf("value is %s\n",BytesToString(values))
	event:=Event{EventHeader{Crc32:BytesToInt32(buf[0:4])},values}
	offset=offset+int64(19+lens)
	fmt.Printf("result value is %d\n",offset)
	return event,offset
}


func (eventLogSet *EventLogSet) ReadBytes(offset int64,fetch int64) ([]byte, int64){
	max:=eventLogSet.BLog.Seek()
	var buffer bytes.Buffer

	if offset>=max{
		return buffer.Bytes(),int64(-1)
	}
	if offset==0{
		offset+=8
	}
	rbytes := make([]byte, fetch)
	bufferdReader := bufio.NewReader(eventLogSet.BLog.file)
	eventLogSet.BLog.SeekReadOffset(rbytes, bufferdReader, offset)
	readcount:=int64(0)
	log.Printf("max is %d offset is %d fetch is %d\n",max,offset,fetch)
	for  {
		if fetch-readcount>27 && readcount+offset<max{
			log.Printf("header is %d %s %d %d %d\n", BytesToInt32(rbytes[readcount:readcount + 4]),
				string(rbytes[readcount + 4:readcount + 5]), BytesToInt64(rbytes[readcount + 5:readcount + 13]),
				BytesToInt32(rbytes[readcount + 13:readcount + 17]), BytesToUint16(rbytes[readcount + 17:readcount + 19]))
			lens := BytesToInt32(rbytes[readcount + 13:readcount + 17])
			if int64(lens) + readcount +19> fetch {
				break
			}
			log.Printf("readcount is %d lens is %d\n",readcount,int64(lens))
			values := rbytes[readcount + 19:readcount + 19 + int64(lens) ]
			buffer.Write(rbytes[readcount:readcount+19])
			buffer.Write(values)
			readcount += 19 + int64(lens)
		}else {
			break
		}
	}
	log.Println("next offset is ",readcount+offset)
	return buffer.Bytes(),readcount+offset
}

func (eventLogSet *EventLogSet) Close()  {
	eventLogSet.BLog.Close()
}
