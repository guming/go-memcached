package main

import (
	"bufio"
	"fmt"
	"net"
	"bytes"
	"strings"
	"strconv"
	"errors"
	"flag"
	"log"

	"memcached/util"
)
var (
	crlf            = []byte("\r\n")
	space           = []byte(" ")
	resultOK        = []byte("OK\r\n")
	resultStored    = []byte("STORED\r\n")
	resultNotStored = []byte("NOT_STORED\r\n")
	resultExists    = []byte("EXISTS\r\n")
	resultNotFound  = []byte("NOT_FOUND\r\n")
	resultDeleted   = []byte("DELETED\r\n")
	resultEnd       = []byte("END\r\n")
	resultOk        = []byte("OK\r\n")
	resultTouched   = []byte("TOUCHED\r\n")
	resultStart        = []byte("VALUE ")
	resultClientErrorPrefix = []byte("CLIENT_ERROR ")
	GET_COMMAND ="get"
	SET_COMMAND ="set"
	GETS_COMMAND ="gets"
)

var(
	ErrMalformedKey = errors.New("key is too long or contains invalid characters")
)

var(
	MARKER_BYTE = 1
	MARKER_BOOLEAN = 8192
	MARKER_INTEGER = 4
	MARKER_LONG = 16384
	MARKER_CHARACTER = 16
	MARKER_STRING = 32
	MARKER_STRINGBUFFER = 64
	MARKER_FLOAT = 128
	MARKER_SHORT = 256
	MARKER_DOUBLE = 512
	MARKER_DATE = 1024
	MARKER_STRINGBUILDER = 2048
	MARKER_BYTEARR = 4096
	MARKER_OTHERS = 0x00
	LESS_TOKENS=5
	MAX_TOKENS=7
	//<command name> <key> <flags> <exptime> <bytes> [noreply]\r\n
	//<command name> cas <key> <flags> <exptime> <bytes> <cas unique> [noreply]\r\n
	//"cas" is a check and set operation which means "store this data but
	//only if no one else has updated since I last fetched it."
	COMMAND_TOKEN=0
	MAX_VALUE_SIZE=1024*1024

)

type Server struct{
	Storage DataStorage
}

type Item struct {
	// Key is the Item's key (250 bytes maximum).
	Key string
	// Value is the Item's value.
	Value []byte
	// Flags are server-opaque flags whose semantics are entirely
	// up to the app.
	Flags uint32
	// Expiration is the cache expiration time, in seconds: either a relative
	// time from now (up to 1 month), or an absolute Unix epoch time.
	// Zero means the Item has no expiration time.
	Expiration int32
	// Compare and swap ID.
	casid uint64
}

type Command struct {
	cname string
}
func main() {
	h:=flag.String("h","127.0.0.1","server ip")
	p:=flag.String("p","11211","server port")
	dir:=flag.String("dir","/Users/guming/dev/research/data/ldb","data dir")
	flag.Parse()
	var tcpAddr *net.TCPAddr
	tcpAddr, _ = net.ResolveTCPAddr("tcp", *h+":"+*p)
	tcpListener, _ := net.ListenTCP("tcp", tcpAddr)
	fmt.Println("server is starting ",*h,*p)
	fmt.Println("service data dir is ",*dir)
	storage:=&LevelDbStorage{}
	canuse:=storage.InitDB(*dir)
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
		fmt.Println("a client connected : " + tcpConn.RemoteAddr().String())
		go handleTcp(tcpConn,storage)
	}
}


func handleTcp(conn *net.TCPConn,stroage DataStorage){
	ipStr := conn.RemoteAddr().String()
	defer func() {
		fmt.Println("disconnected :" + ipStr)
		conn.Close()
	}()

	reader := bufio.NewReader(conn)

	for {

		str,err:= reader.ReadBytes('\n')
		if err != nil {
			//log.Println(err)
			return
		}

		str = bytes.TrimRight(str, "\r\n")
		if len(str)<=0{
			return
		}
		tokens,n:=tokenize_command(str)
		command:=tokens[COMMAND_TOKEN]
		if !legalKey(command){
			return
		}
		var buffer bytes.Buffer
		if strings.EqualFold(command,SET_COMMAND) {
			if(n<LESS_TOKENS){
				return
			}
			var buffer_persistence bytes.Buffer
			value, err := reader.ReadBytes('\n')
			if err != nil {
				return
			}
			if len(value)>MAX_VALUE_SIZE{
				return
			}

			value = bytes.TrimRight(value, "\r\n")
			fmt.Println("set command:",value)
			flags,err:=strconv.ParseUint(tokens[2],0,32)
			if err!=nil{
				return
			}
			flags32:=uint32(flags)
			expiration,err:=strconv.ParseInt(tokens[3],0,32)
			if err!=nil{
				return
			}
			expiration32:=int32(expiration)
			item:=&Item{
				Key:tokens[1],
				Flags:flags32,
				Expiration:expiration32,
				Value:value,
			}
			buffer_persistence.Write(util.Unit32ToBytes(item.Flags))
			buffer_persistence.Write(util.Int32ToBytes(item.Expiration))
			buffer_persistence.Write(value)
			fmt.Println("value len:",len(value))
			err = stroage.Put([]byte(tokens[1]), buffer_persistence.Bytes())
			if err != nil {
				fmt.Println(err)
				return
			}
			buffer.Write(resultStored)
		}
		if strings.EqualFold(tokens[COMMAND_TOKEN],GET_COMMAND) {
			fmt.Println("get command",tokens[1])
			result, err := stroage.Get([]byte(tokens[1]))
			fmt.Println("result:",result)
			if err != nil ||len(result)<8 {
				fmt.Println(err)
				return
			}
			writebuffer(&buffer,tokens[1],result)
		}
		if strings.EqualFold(tokens[COMMAND_TOKEN], GETS_COMMAND) {
			fmt.Println("gets command",tokens[1])
			result, err := stroage.Get([]byte(tokens[1]))
			if err != nil||len(result)<8 {
				fmt.Println("err",err)
				return
			}
			writebuffer(&buffer,tokens[1],result)
		}
		conn.Write(buffer.Bytes())
	}
}
func writebuffer(buffer *bytes.Buffer, key string, result []byte) {
	flags:=result[0:4]
	value:=result[8:]
	buffer.Write(resultStart)
	buffer.WriteString(key)
	buffer.Write(space)
	buffer.WriteString(strconv.Itoa(int(util.BytesToUint32(flags))))
	buffer.Write(space)
	buffer.WriteString(strconv.Itoa(len(value)))
	buffer.Write(crlf)
	buffer.Write(value)
	buffer.Write(crlf)
	buffer.Write(resultEnd)
}


func tokenize_command(str []byte) (tokens []string,n int){
	tokens=strings.Fields(string(str))
	n=len(tokens)
	log.Println("token size ",n)
	return
}

func process_command(buffer *bytes.Buffer,command string,reader *bufio.Reader,tokens []string,n int,storage DataStorage){
	if strings.EqualFold(command,SET_COMMAND) {
		datas,err:=process_set_command(n,reader,tokens)
		if err!=nil{
			buffer.Write(datas)
			return
		}
		err = storage.Put([]byte(tokens[1]), datas)
		if err != nil {
			fmt.Println(err)
			return
		}
		buffer.Write(resultStored)
	}
	if strings.EqualFold(tokens[COMMAND_TOKEN],GET_COMMAND) {
		fmt.Println("get command",tokens[1])
		result, err := storage.Get([]byte(tokens[1]))
		fmt.Println("result:",result)
		if err != nil ||len(result)<8 {
			fmt.Println(err)
			return
		}
		writebuffer(buffer,tokens[1],result)
	}
	if strings.EqualFold(tokens[COMMAND_TOKEN], GETS_COMMAND) {
		fmt.Println("gets command",tokens[1])
		result, err := storage.Get([]byte(tokens[1]))
		if err != nil||len(result)<8 {
			fmt.Println("err",err)
			return
		}
		writebuffer(buffer,tokens[1],result)
	}
}

func process_set_command(n int,reader *bufio.Reader,tokens []string) (result []byte,err error){
	if(n<LESS_TOKENS){
		return resultClientErrorPrefix,ErrMalformedKey
	}
	var buffer_persistence bytes.Buffer
	value, err := reader.ReadBytes('\n')
	if err != nil {
		return resultClientErrorPrefix,err
	}
	if len(value)>MAX_VALUE_SIZE||len(value)<=0{
		return resultClientErrorPrefix,nil
	}

	value = bytes.TrimRight(value, "\r\n")
	fmt.Println("set command:",value)
	flags,err:=strconv.ParseUint(tokens[2],0,32)
	if err!=nil{
		return resultClientErrorPrefix,err
	}
	flags32:=uint32(flags)
	expiration,err:=strconv.ParseInt(tokens[3],0,32)
	if err!=nil{
		return resultClientErrorPrefix,err
	}
	expiration32:=int32(expiration)
	item:=&Item{
		Key:tokens[1],
		Flags:flags32,
		Expiration:expiration32,
		Value:value,
	}
	buffer_persistence.Write(util.Unit32ToBytes(item.Flags))
	buffer_persistence.Write(util.Int32ToBytes(item.Expiration))
	buffer_persistence.Write(value)
	fmt.Println("value len:",len(value))
	return buffer_persistence.Bytes(),nil
}

func legalKey(key string) bool {
	if len(key) > 250 {
		return false
	}
	for i := 0;i < len(key);i++ {
		if key[i] <= ' ' || key[i] == 0x7f {
			return false
		}
	}
	return true
}
