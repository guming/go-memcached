package binlog

import (
	"net"
	"fmt"
	"bytes"
	"strings"
	"bufio"
	"strconv"
)

var (
	crlf            = []byte("\r\n")
	space           = []byte(" ")
	resultOK        = []byte("OK\r\n")
	resultStored    = []byte("SYNCED\r\n")
	resultNotStored = []byte("NOT_STORED\r\n")
	resultExists    = []byte("EXISTS\r\n")
	resultNotFound  = []byte("NOT_FOUND\r\n")
	resultDeleted   = []byte("DELETED\r\n")
	resultEnd       = []byte("END\r\n")
	resultOk        = []byte("OK\r\n")
	resultTouched   = []byte("TOUCHED\r\n")
	resultStart        = []byte("VALUE ")
	resultClientErrorPrefix = []byte("CLIENT_ERROR ")
	SYNC_COMMAND ="sync"
)

func StartSync(){
	var tcpAddr *net.TCPAddr
	tcpAddr, _ = net.ResolveTCPAddr("tcp", "172.16.30.18:9998")
	tcpListener, _ := net.ListenTCP("tcp", tcpAddr)
	defer tcpListener.Close()
	for {
		tcpConn, err := tcpListener.AcceptTCP()
		if err != nil {
			continue
		}
		fmt.Println("A client connected : " + tcpConn.RemoteAddr().String())
		go handleTcp(tcpConn)
	}
}

func handleTcp(conn *net.TCPConn) {
	eventLogSet:=LoadEventSet("test.bin")
	ipStr := conn.RemoteAddr().String()
	defer func() {
		fmt.Println("disconnected :" + ipStr)
		conn.Close()
	}()
	reader := bufio.NewReader(conn)

	for {
		key,err:= reader.ReadBytes('\n')
		key = bytes.TrimRight(key, "\r\n")
		str:=strings.Fields(string(key))
		if err != nil {
			return
		}
		var buffer bytes.Buffer
		if strings.EqualFold(str[0],SYNC_COMMAND) {
			fmt.Printf("offset is %s,fetch is %s\n", str[1], str[2])
			offset, _ := strconv.ParseInt(str[1], 0, 64)
			fetch, _ := strconv.ParseInt(str[2], 0, 64)
			values, position := eventLogSet.ReadBytes(offset, fetch)
			buffer.Write(Int64ToBytes(position))
			buffer.Write(crlf)
			buffer.Write(values)
			buffer.Write(crlf)
		}
		conn.Write(buffer.Bytes())
	}
}