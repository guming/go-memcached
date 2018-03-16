package binlog

import (
	"os"
	"log"
	"io"
	"strconv"
	"sync"
)

var File_Header=[]byte("rock0100")
var LOG_MAX_SIZE=int64(1024*1024*1024)

type logfile struct {
	Startsize int64
	Filename string
	file *os.File
	mutex sync.Mutex
}

type EventLogSet struct {
	BLog *logfile
}

func (l *logfile) Rolling() bool{
	l.mutex.Lock()
	defer l.mutex.Unlock()
	filesize:=l.Seek()
	if filesize>=LOG_MAX_SIZE {
		filename := l.Filename
		l.Close()
		index := filename[len(filename)-1:]
		i, _ := strconv.Atoi(index)
		filename = filename + strconv.Itoa(i+1)
		var file *os.File
		file, err := os.OpenFile(filename, os.O_RDWR|os.O_APPEND, 0666)

		if os.IsNotExist(err) {
			file, err = os.OpenFile(filename, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0666)
			if err != nil {
				log.Fatal("error", err)
				l.file = nil
				return false
			}
			file.Write(File_Header)
		}
		l.file = file

		return true
	}else{
		//no need to rolling
		return true
	}
}

func (l *logfile) Close(){
	l.file.Close()
}

func (l *logfile) File() *os.File{
	return l.file
}

func NewRWlog(filename string) *logfile {
	var file *os.File
	file,err:=os.OpenFile(filename,os.O_RDWR|os.O_APPEND,0666)

	if os.IsNotExist(err){
		file,err=os.OpenFile(filename,os.O_CREATE|os.O_RDWR|os.O_APPEND,0666)
		if err!=nil{
			log.Println(filename)
			log.Fatal("error",err)
			return nil
		}
		file.Write(File_Header)
	}

	return &logfile{Filename:filename,file:file}
}

func LoadReadlog(filename string,startsize int64) *logfile {
	var file *os.File
	file,err:=os.OpenFile(filename,os.O_RDONLY|os.O_APPEND,0666)
	if err!=nil {
		log.Fatalln("error",err)
		return nil
	}
	file.Seek(8,0)
	return &logfile{Filename:filename,file:file,Startsize:startsize}
}

func (log *logfile) SeekRead(bytes []byte,rio io.Reader) {
	rio.Read(bytes)
}

func (log *logfile) SeekReadOffset(bytes []byte,rio io.Reader,offset int64) {
	log.file.Seek(offset,0)
	rio.Read(bytes)
}

func (logfile *logfile) Seek() int64{
	ret,err:=logfile.file.Seek(0,io.SeekEnd)
	if err!=nil{
		log.Fatalln(err)
		return -1
	}
	return ret
}

