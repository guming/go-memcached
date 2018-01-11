package main

import (
	"bufio"
	"net"
	"flag"
	"log"
)


type Item struct {
	// Key is the Item's key (250 bytes maximum).
	Key string
	Value []byte
	Flags uint32
	// Expiration is the cache expiration time, in seconds: either a relative
	// time from now (up to 1 month), or an absolute Unix epoch time.
	// Zero means the Item has no expiration time.
	Expiration int32
	casid uint64
}

func main() {
	h:=flag.String("h","127.0.0.1","server ip")
	p:=flag.String("p","11211","server port")
	dir:=flag.String("dir","/Users/guming/dev/research/data/ldb","data dir")
	protocol:=flag.String("protocol","ascii","trans protocol")
	flag.Parse()
	var tcpAddr *net.TCPAddr
	tcpAddr, _ = net.ResolveTCPAddr("tcp", *h+":"+*p)
	tcpListener, _ := net.ListenTCP("tcp", tcpAddr)
	log.Println("server is starting ",*h,*p)
	log.Println("service data dir is ",*dir)
	storage:=&LevelDbStorage{}
	canuse:=storage.InitDB(*dir)
	if *protocol=="ascii"{
		log.Println("protocol ",*protocol)
	}else {
		log.Println("not support the protocol ",*protocol)
		return
	}
	proto:=&AsciiProtocol{
		Storage:storage,
	}
	if !canuse{
		return
	}
	defer tcpListener.Close()
	defer storage.Close()
	for {
		tcpConn, err := tcpListener.AcceptTCP()
		if err != nil {
			continue
		}
		log.Println("client connected : " + tcpConn.RemoteAddr().String())

		go handleTcp(tcpConn,proto)
	}
}


func handleTcp(conn *net.TCPConn,proto Protocol){
	ipStr := conn.RemoteAddr().String()
	defer func() {
		log.Println("disconn :" + ipStr)
		conn.Close()
	}()

	reader := bufio.NewReader(conn)

	for {
		source,err:= reader.ReadBytes('\n')
		if err != nil {
			return
		}

		back,err:=proto.process_command(source,reader)
		if err!=nil{
			log.Println(err)
			if err==ErrClientError{
				conn.Write(resultClientErrorPrefix)
			}else {
				conn.Write(resultServerErrorPrefix)
			}
		}
		conn.Write(back)
	}
}



