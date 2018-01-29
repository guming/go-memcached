package main

import (
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/google/btree"
	"time"
	"log"
	"bytes"
	"memcached/binlog"
	"sync"
)
var btreeDegree = 32

type DataStorage interface {
	//InitDB(dir string) bool
	Put(key []byte,value []byte,expiration int64) error
	Get(key []byte) (value []byte,err error)
	Close()
}

type LevelDbStorage struct{
	Db *leveldb.DB
	Index *btree.BTree
	blog *binlog.EventLogSet
	synclog *binlog.EventLogSet
	mu sync.Mutex
}

type Node struct{
	expiration int64
	key []byte
}
func (n *Node) Less(than btree.Item) bool{
	if than ==nil{
		return true
	}
	return bytes.Compare(n.key,than.(*Node).key)==-1
}

func (lds *LevelDbStorage) InitDB(dir string) bool {
	db,err:=leveldb.OpenFile(dir, nil)
	if err != nil {
		return false
	}
	lds.Db=db
	lds.Index = btree.New(btreeDegree)
	blogname:=dir+"/log.bin"
	lds.blog=binlog.NewEventSet(blogname)
	lds.synclog=binlog.LoadEventSet(blogname)
	return true
}

func (lds *LevelDbStorage) Put(key []byte,value []byte,expiration int64) error{
	err:=lds.Db.Put(key,value,nil)
	if err==nil{
		node:=&Node{
			key:key,
			expiration:expiration,
		}
		lds.mu.Lock()
		lds.Index.ReplaceOrInsert(node)
		lds.mu.Unlock()
		eventheader:=binlog.EventHeader{int32(len(key)),'0',0,int32(len(value)+len(key)),01}
		var buffer bytes.Buffer
		buffer.Write(key)
		buffer.Write(value)
		event:=binlog.Event{eventheader,buffer.Bytes()}
		lds.blog.WriteEvent(event)
	}
	return err
}

func (lds *LevelDbStorage) Get(key []byte) (value []byte,err error){
	//log.Println("unix curr time:",time.Now().Unix())
	cur_time:=time.Now().Unix()
	node:=&Node{
		key:key,
	}
	item:=lds.Index.Get(node)
	if item!=nil {
		inode := item.(*Node)
		if inode.expiration>0 && cur_time > inode.expiration {
			log.Println("expiration index:",inode.expiration)
			err:=lds.Db.Delete(key,nil)
			if err!=nil{
				log.Println("after expiration remove error:",err,string(key))
			}
			return nil, nil
		}
	}
	value,err=lds.Db.Get(key,nil)

	//log.Println(value)
	if err==nil && len(value)>12{
		expir:=value[4:12]
		expir_int:=int64(BytesToUint64(expir))
		if expir_int>0 && cur_time>expir_int{
			log.Println("expiration leveldb:",expir_int)
			err:=lds.Db.Delete(key,nil)
			if err!=nil{
				log.Println("after expiration remove error:",err,string(key))
			}
			return nil,nil
		}
	}
	return value,err
}

func (lds *LevelDbStorage) Close(){
	lds.Db.Close()
	lds.blog.Close()
	lds.synclog.Close()
}