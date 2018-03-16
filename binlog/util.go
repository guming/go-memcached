package binlog

import (
	"encoding/binary"
	"strings"
	"fmt"
	"strconv"
	"path/filepath"
	"sort"
	"os"
)

func BytesToString(buf []byte) string {
	return string(buf)
}

func BytesToInt32(buf []byte) int32 {
	return int32(binary.BigEndian.Uint32(buf))
}

func BytesToInt64(buf []byte) int64 {
	return int64(binary.BigEndian.Uint64(buf))
}

func BytesToUint16(buf []byte) uint16 {
	return uint16(binary.BigEndian.Uint16(buf))
}

func Int32ToBytes(i int32) []byte {
	var buf = make([]byte, 4)
	binary.BigEndian.PutUint32(buf,uint32(i))
	return buf
}

func Int64ToBytes(i int64) []byte {
	var buf = make([]byte, 8)
	binary.BigEndian.PutUint64(buf,uint64(i))
	return buf
}

func UnitToBytes(i uint16) []byte {
	var buf = make([]byte, 2)
	binary.BigEndian.PutUint16(buf,i)
	return buf
}

var logarray []int64

var current_logfile string

func walkFunc(path string, info os.FileInfo,err error) error {
	if info == nil {
		return nil
	}
	if info.IsDir() {
		return nil
	} else {
		str:=strings.Split(path,"/")
		filename:=str[len(str)-1]
		if strings.Contains(filename,".bin") {
			str1:=strings.Split(filename,".")
			fmt.Println(filename)
			filename=str1[0]
			idx,_:=strconv.Atoi(filename)
			logarray=append(logarray,int64(idx))
			current_logfile=path
		}
		return nil
	}
}

func showMaxfileIdx(path string) (int64,string) {
	err := filepath.Walk(path, walkFunc)
	if err != nil {
		fmt.Printf("filepath.Walk() error: %v\n", err)
	}
	sort.Slice(logarray, func(i, j int) bool {
		return logarray[i] > logarray[j]
	})
	if len(logarray)>0 {
		fmt.Printf("file idx:%d\r\n", logarray[0])
		return logarray[0],current_logfile
	}else{
		return -1,current_logfile
	}
}