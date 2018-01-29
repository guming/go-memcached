package main

import (
	"bufio"
	"bytes"
	"strings"
	"log"
	"strconv"
	"errors"
	"time"
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
	resultServerErrorPrefix = []byte("SERVER_ERROR ")
	resultBadCommand = []byte("bad command line\r\n")
	resultKeyError = []byte("key is too long 250 limit\r\n")
	resultValueError = []byte("value is too long, 1mb limit\r\n")
	resultStoredError = []byte("stored error\r\n")
	resultReadedError = []byte("read error\r\n")
	GET_COMMAND ="get"
	SET_COMMAND ="set"
	GETS_COMMAND ="gets"
)

var(
	ErrNotStored = errors.New("memcache: item not stored")
	ErrServerError = errors.New("server error")
	ErrNoStats = errors.New("memcache: no statistics available")
	ErrMalformedKey = errors.New("malformed: key is too long or contains invalid characters")
	ErrClientError = errors.New("client error")
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
	MIN_TOKENS=5
	MAX_TOKENS=7
	//<command name> <key> <flags> <exptime> <bytes> [noreply]\r\n
	//<command name> cas <key> <flags> <exptime> <bytes> <cas unique> [noreply]\r\n
	//"cas" is a check and set operation which means "store this data but
	//only if no one else has updated since I last fetched it."
	COMMAND_TOKEN=0
	MAX_VALUE_SIZE=1024*1024
	TIME_THRITY_DAYS=60*60*24*30

)

type Protocol interface {
	IsBinaryProtocol() bool
	process_command(source []byte,reader *bufio.Reader)([]byte,error)
	IsReadOnly() bool
	SetReadOnly(flag bool)
}

type AsciiProtocol struct{
	Storage DataStorage
	readflag bool
}

func (ascii *AsciiProtocol) SetReadOnly(flag bool) {
	ascii.readflag=flag
}

func (ascii *AsciiProtocol) IsReadOnly() bool {
	return ascii.readflag
}

func (ascii *AsciiProtocol) IsBinaryProtocol() bool {
	return false
}

func (ascii *AsciiProtocol) process_command(source []byte,reader *bufio.Reader)([]byte,error){

	source = bytes.TrimRight(source, "\r\n")
	if len(source)<=0{
		return resultBadCommand,ErrClientError
	}

	tokens,n:=tokenize_command(source)
	command:=tokens[COMMAND_TOKEN]
	if !legalKey(command){
		return resultKeyError,ErrClientError
	}

	var buffer bytes.Buffer
	if(!ascii.IsReadOnly()) {
		if strings.EqualFold(command, SET_COMMAND) {
			datas, expiration, err := process_set_command(n, reader, tokens)
			if err != nil {
				log.Println(string(datas))
				buffer.Write(datas)
			} else {
				err = ascii.Storage.Put([]byte(tokens[1]), datas, expiration)
				if err != nil {
					log.Println("error put", err)
					return resultStoredError, ErrServerError
				}

				buffer.Write(resultStored)
			}
		}
	}
	if strings.EqualFold(tokens[COMMAND_TOKEN],GET_COMMAND) {
		//log.Println("get command",tokens[1])
		result, err := ascii.Storage.Get([]byte(tokens[1]))
		//log.Println("result:",result)
		if err != nil{
			log.Println("err",err)
			return nil,nil
		}
		if result==nil||len(result)<8 {
			return nil,nil
		}
		writebuffer(&buffer,tokens[1],result)
	}
	if strings.EqualFold(tokens[COMMAND_TOKEN], GETS_COMMAND) {
		//log.Println("gets command",tokens[1])
		result, err := ascii.Storage.Get([]byte(tokens[1]))
		if err != nil{
			log.Println("err",err)
			return nil,nil
		}
		if result==nil||len(result)<8 {
			return nil,nil
		}
		writebuffer(&buffer,tokens[1],result)
	}
	return buffer.Bytes(),nil
}




func tokenize_command(str []byte) (tokens []string,n int){
	tokens=strings.Fields(string(str))
	n=len(tokens)
	return
}

func process_set_command(n int,reader *bufio.Reader,tokens []string) (result []byte,expiration32 int64,err error){
	if(n<MIN_TOKENS){
		return resultBadCommand,-1,ErrClientError
	}
	var buffer_persistence bytes.Buffer
	value, err := reader.ReadBytes('\n')
	if err != nil {
		log.Println("reader error ",err)
		return resultBadCommand,-1,ErrServerError
	}
	if len(value)>MAX_VALUE_SIZE||len(value)<=0{
		return resultValueError,-1,ErrClientError
	}

	value = bytes.TrimRight(value, "\r\n")
	flags,err:=strconv.ParseUint(tokens[2],0,32)
	//log.Println("flags:",flags)
	if err!=nil{
		log.Println("strconv ParseUint error ",err)
		return resultBadCommand,-1,ErrClientError
	}
	flags32:=uint32(flags)
	expiration,err:=strconv.ParseInt(tokens[3],0,32)
	//log.Println("expiration:",expiration)
	//if time is offset then change it to the true time
	if expiration>0&&expiration<int64(TIME_THRITY_DAYS){
		//log.Println("unix curr time:",time.Now().Unix())
		expiration=expiration+time.Now().Unix()
	}

	if err!=nil{
		log.Println("strconv ParseUint error ",err)
		return resultBadCommand,-1,ErrClientError
	}
	//expiration32=int32(expiration)
	item:=&Item{
		Key:tokens[1],
		Flags:flags32,
		Expiration:expiration,
		Value:value,
	}
	buffer_persistence.Write(Unit32ToBytes(item.Flags))
	buffer_persistence.Write(Int64ToBytes(item.Expiration))
	buffer_persistence.Write(value)
	return buffer_persistence.Bytes(),item.Expiration,nil
}

func writebuffer(buffer *bytes.Buffer, key string, result []byte) {
	flags:=result[0:4]
	value:=result[12:]
	buffer.Write(resultStart)
	buffer.WriteString(key)
	buffer.Write(space)
	buffer.WriteString(strconv.Itoa(int(BytesToUint32(flags))))
	buffer.Write(space)
	buffer.WriteString(strconv.Itoa(len(value)))
	buffer.Write(crlf)
	buffer.Write(value)
	buffer.Write(crlf)
	buffer.Write(resultEnd)
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