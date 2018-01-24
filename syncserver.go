package main

import (
	"net"
	"fmt"
	"bytes"
	"strings"
	"bufio"
	"strconv"
	"memcached/binlog"
	"log"
)

var (

	SYNC_COMMAND ="sync"
)

func StartSync(){
	var tcpAddr *net.TCPAddr
	tcpAddr, _ = net.ResolveTCPAddr("tcp", "127.0.0.1:9998")
	tcpListener, _ := net.ListenTCP("tcp", tcpAddr)
	defer tcpListener.Close()
	for {
		tcpConn, err := tcpListener.AcceptTCP()
		if err != nil {
			continue
		}
		fmt.Println("the client connected : " + tcpConn.RemoteAddr().String())
		go handleSlaveTcp(tcpConn)
	}
}

func handleSlaveTcp(conn *net.TCPConn) {
	eventLogSet:=binlog.LoadEventSet("/Users/guming/dev/research/data/ldb/log.bin")
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
		log.Println("command:",string(key))
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