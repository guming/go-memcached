package test

import (
	"os"
	"fmt"
	"log"
	"bufio"
)

func main()  {
	file,err:=os.OpenFile("test.txt",os.O_RDWR|os.O_APPEND,700)
	if err!=nil{
		return
	}
	//file.WriteString("hello world.\r\n")
	//fmt.Println(file.Name())
	rbytes:=make([]byte,8)
	bytesRead,err:=file.Read(rbytes)
	log.Printf("Number of bytes read: %d\n", bytesRead)
	log.Printf("Data read: %s\n", rbytes)

	//bytes,err:=ioutil.ReadFile("test.txt")
	bufferedReader := bufio.NewReader(file)
	rbyteSlice := make([]byte, 16)
	bufferedReader.Read(rbyteSlice)
	log.Printf("Data read: %s\n", rbyteSlice)
	byteSlice := []byte("bytes!\n")
	bytesWritten, err := file.Write(byteSlice)
	log.Printf("Wrote %d bytes.\n", bytesWritten)
	defer file.Close()
	var offset int64 =10
	var whence int =2
	newPosition,err:=file.Seek(offset,whence)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(newPosition)
	bufferwriter:=bufio.NewWriter(file)

	bytesWritten2,err:=bufferwriter.Write([]byte("hi\n"))
	log.Printf("buf Wrote %d bytes.\n", bytesWritten2)
	unflushedBufferSize:=bufferwriter.Buffered()
	log.Println("unflushedBufferSize ",unflushedBufferSize)
	bytesAvailable := bufferwriter.Available()
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Available buffer: %d\n", bytesAvailable)
	bufferwriter.Flush()
	num:=bufferwriter.Buffered()
	log.Println("unflushedBufferSize ",num)
	//reset bufsize
	bufferwriter = bufio.NewWriterSize(
		bufferwriter,
		8192,
	)
	// resize后检查缓存的大小
	bytesAvailable = bufferwriter.Available()
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Available buffer: %d\n", bytesAvailable)



}
