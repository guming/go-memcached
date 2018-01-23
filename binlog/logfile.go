package binlog

import (
	"os"
	"log"
	"io"
)

type logfile struct {
	filename string
	file *os.File
}

type EventLogSet struct {
	bLog *logfile
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
	var File_Header=[]byte("rock0100")
	if os.IsNotExist(err){
		file,err=os.OpenFile(filename,os.O_CREATE|os.O_RDWR|os.O_APPEND,0666)
		if err!=nil{
			log.Fatal("error",err)
			return nil
		}
		file.Write(File_Header)
	}

	return &logfile{filename,file}
}

func NewReadlog(filename string) *logfile {
	var file *os.File
	file,err:=os.OpenFile(filename,os.O_RDONLY|os.O_APPEND,0666)
	if err!=nil{
		log.Fatalln("error",err)
		return nil
	}
	file.Seek(8,0)
	return &logfile{filename,file}
}

func (log *logfile) SeekRead(bytes []byte,rio io.Reader) {
	rio.Read(bytes)
}

func (log *logfile) SeekReadOffset(bytes []byte,rio io.Reader,offset int64) {
	log.file.Seek(offset,0)
	rio.Read(bytes)
}

func (logfile *logfile) Seek() int64{
	ret,err:=logfile.file.Seek(0,os.SEEK_END)
	if err!=nil{
		log.Fatalln(err)
		return -1
	}
	return ret
}

