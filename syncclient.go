package binlog

import (
	"net"
	"time"
	"fmt"
	"bufio"
	"bytes"
	"log"
)


var quitSemaphore chan bool

func StartToSync() {
	conn, err := net.DialTimeout("tcp", "172.16.30.18:9998", time.Second*2)
	if err != nil {
		fmt.Println("dial error:", err)
		return
	}
	defer conn.Close()
	fmt.Println("connected!")
	go onMessageRecived(conn)
	go fetchLog(conn)
	<-quitSemaphore
}

func fetchLog(conn net.Conn){
	for {
		var buffer bytes.Buffer
		buffer.Write([]byte("sync "))
		//buffer.Write(st)
		buffer.Write([]byte(" 1024\r\n"))
		conn.Write(buffer.Bytes())
		time.Sleep(time.Millisecond*50)
	}
}

func onMessageRecived(conn net.Conn) {
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
		offset:=BytesToInt64(msg)
		fmt.Println(offset)

		msg, err = reader.ReadBytes('\n')
		msg = bytes.TrimRight(msg, "\r\n")
		events:=UnPackEvents(msg)
		for iter := events.Front();iter != nil ;iter = iter.Next() {
			fmt.Println("item:",iter.Value)
		}
	}
}
