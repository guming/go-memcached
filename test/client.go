package test

import (
	"bufio"
	"fmt"
	"net"
	"time"
	"bytes"
)

var quitSemaphore1 chan bool

func main() {
	//var tcpAddr *net.TCPAddr
	//tcpAddr, _ = net.ResolveTCPAddr("tcp", "192.168.7.132:9999")
	conn, err := net.DialTimeout("tcp", "192.168.7.132:9999", time.Second*2)
	if err != nil {
		fmt.Println("dial error:", err)
		return
	}
	defer conn.Close()
	fmt.Println("connected!")
	go onMessageRecived(conn)
	b := []byte("set test 0 0 2\r\nmytest\r\n")
	conn.Write(b)
	for {
		c := []byte("get test\r\n")
		conn.Write(c)
		//time.Sleep(time.Second)
	}
	<-quitSemaphore1
}

func onMessageRecived(conn net.Conn) {
	fmt.Println("onMessageRecived!")
	//conn.SetReadDeadline(time.Now().Add(2*time.Second))
	reader := bufio.NewReader(conn)
	for {
		msg, err := reader.ReadBytes('\n')
		msg = bytes.TrimRight(msg, "\r\n")
		fmt.Println(string(msg))
		if err != nil {
			quitSemaphore1 <- true
			break
		}
	}
}
