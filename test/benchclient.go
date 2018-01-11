package test

import (
	"bufio"
	"fmt"
	"net"
	"bytes"
)

var quitSemaphore chan bool
var (
	MaxClient      = 1000
)
func main() {
	var tcpAddr *net.TCPAddr
	tcpAddr, _ = net.ResolveTCPAddr("tcp", "192.168.7.132:9999")
	for i := 1; i <= MaxClient; i++ {
		conn, err := net.DialTCP("tcp", nil, tcpAddr)
		if err!=nil{
			fmt.Println(err)
			return
		}
		defer conn.Close()

		fmt.Println("connected!")
		go onMRecived(conn)
		c := []byte("get test\r\n")
		conn.Write(c)
	}
	<-quitSemaphore
}
func onMRecived(conn *net.TCPConn) {
	reader := bufio.NewReader(conn)
	for {
		msg, err := reader.ReadBytes('\n')
		if err != nil {
			quitSemaphore <- true
			break
		}
		msg = bytes.TrimRight(msg, "\r\n")
		fmt.Println(string(msg))
	}
}