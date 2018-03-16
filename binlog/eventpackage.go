package binlog

import (
	"container/list"
	"log"
)

func UnPackEvents(rbytes []byte) (*list.List,[]byte) {
	size:=int64(len(rbytes))
	log.Println("bytes size:",size)
	log.Println("bytes string:",string(rbytes))
	readcount:=int64(0)
	events:=list.New()
	for  {
		log.Println("readcount:",readcount)

		if  size-readcount>19{
			log.Printf("header is %d %s %d %d %d\n", BytesToInt32(rbytes[readcount:readcount + 4]),
				string(rbytes[readcount + 4:readcount + 5]), BytesToInt64(rbytes[readcount + 5:readcount + 13]),
				BytesToInt32(rbytes[readcount + 13:readcount + 17]), BytesToUint16(rbytes[readcount + 17:readcount + 19]))
			lens := BytesToInt32(rbytes[readcount + 13:readcount + 17])
			crc32:=BytesToInt32(rbytes[readcount:readcount + 4])
			if size-readcount-19>int64(lens+crc32) {
				values := rbytes[readcount+19:readcount+19+int64(lens)]
				log.Println("key lens:", crc32)
				log.Println("value lens:", lens)
				event := Event{EventHeader{Crc32: crc32}, values}
				events.PushBack(event)
				readcount += 19 + int64(lens)
			}else{
				num:=size-readcount
				lestbytes:=rbytes[size-num:]
				return events,lestbytes
			}
		}else{
			num:=size-readcount
			lestbytes:=rbytes[size-num:]
			return events,lestbytes
		}
	}
	return events,nil
}