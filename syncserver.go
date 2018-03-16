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
	//"os"
	//"path/filepath"
	"iodemo/util"
)

var (

	SYNC_COMMAND ="sync"
)

func StartSync(binlog_dir string,storage DataStorage){
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
		go handleSlaveTcp(tcpConn,binlog_dir,storage)
	}
}

func handleSlaveTcp(conn *net.TCPConn,binlog_dir string,storage DataStorage) {
	eventLogSet:=binlog.LoadEventSet(binlog_dir)
	ipStr := conn.RemoteAddr().String()
	defer func() {
		fmt.Println("disconnected :" + ipStr)
		conn.Close()
	}()
	reader := bufio.NewReader(conn)
	nkey:=ipStr+"_offset"
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
			log.Printf("offset is %s,fetch is %s\n", str[1], str[2])
			//offset, _ := strconv.ParseInt(str[1], 0, 64)
			//if offset>0{
			//	offset-=eventLogSet.BLog.Startsize
			//}
			//log.Println("offset is ",offset)
			fetch, _ := strconv.ParseInt(str[2], 0, 64)
			v,_:=storage.Get([]byte(nkey))
			offset:=int64(0)
			if v!=nil{
				offset=util.BytesToInt64(v)
			}
			values, position := eventLogSet.ReadBytes(offset, fetch)
			position+=eventLogSet.BLog.Startsize
			log.Printf("Startsize is %d value size is %d \n",position,len(values))
			buffer.Write(Int64ToBytes(position))
			buffer.Write(crlf)
			buffer.Write(values)
			buffer.Write(crlf)
			conn.Write(buffer.Bytes())

			storage.Put([]byte(nkey),util.Int64ToBytes(position),0)
		}
	}
}



