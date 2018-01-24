package main

import (
	"net"
	"time"
	"fmt"
	"bufio"
	"bytes"
	"log"
	"memcached/binlog"
	"sync"
	"strconv"
)


var quitSemaphore chan bool
var mu sync.Mutex
var offset=int64(0)
func StartToSync(storage DataStorage) {
	conn, err := net.DialTimeout("tcp", "127.0.0.1:9998", time.Second*2)
	if err != nil {
		fmt.Println("dial error:", err)
		return
	}
	defer conn.Close()
	fmt.Println("connected!")
	go onMessageRecived(conn,storage)
	go fetchLog(conn)
	<-quitSemaphore
}

func fetchLog(conn net.Conn){
	for {
		var buffer bytes.Buffer
		buffer.Write([]byte("sync "))
		//buffer.Write(st)
		value:=getOffset()
		//log.Println("getOffset",strconv.Itoa(int(value)))
		buffer.Write([]byte(strconv.Itoa(int(value))+" 1024\r\n"))
		conn.Write(buffer.Bytes())
		time.Sleep(time.Millisecond*100)
	}
}

func setOffset(num int64){
	mu.Lock()
	defer mu.Unlock()
	offset=num
}

func getOffset() int64{
	mu.Lock()
	defer mu.Unlock()
	return offset
}

func onMessageRecived(conn net.Conn,storage DataStorage) {
	fmt.Println("onMessageRecived!")
	//conn.SetReadDeadline(time.Now().Add(2*time.Second))
	reader := bufio.NewReader(conn)
	for {
		msg, err := reader.ReadBytes('\n')
		if err != nil {
			log.Fatalln(err)
			quitSemaphore <- true
			break
		}
		msg = bytes.TrimRight(msg, "\r\n")
		offset_back:=BytesToInt64(msg)
		if offset_back>getOffset(){
			setOffset(offset_back)
		}

		msg, err = reader.ReadBytes('\n')
		msg = bytes.TrimRight(msg, "\r\n")
		events:= binlog.UnPackEvents(msg)
		for iter := events.Front();iter != nil ;iter = iter.Next() {
			fmt.Println("item:",string(iter.Value.(binlog.Event).EventData))
			eventheader:=iter.Value.(binlog.Event).EventHeader
			eventdata:=iter.Value.(binlog.Event).EventData
			if len(eventdata)>0 {
				key := eventdata[0:eventheader.Crc32]
				value := eventdata[eventheader.Crc32:]
				if(value!=nil && len(value)>12) {
					expir := value[4:12]
					expir_int := int64(BytesToUint64(expir))
					err := storage.Put(key, value, expir_int)
					if err != nil {
						log.Println("slave sync error key is %s", string(key))
						log.Fatal(err)
					}
				}else{
					log.Println("eventdata(value len)<12,key is",string(key))
				}
			}else{
				log.Println("eventdata is nil,offset is",offset_back)
			}
		}
	}
}
