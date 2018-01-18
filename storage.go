package main

import (
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/google/btree"
	"time"
	"log"
	"bytes"
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
}

type Node struct{
	expiration int64
	key []byte
}
func (n *Node) Less(than btree.Item) bool{
	return bytes.Compare(n.key,than.(*Node).key)==-1
}

func (lds *LevelDbStorage) InitDB(dir string) bool {
	db,err:=leveldb.OpenFile(dir, nil)
	if err != nil {
		return false
	}
	lds.Db=db
	lds.Index = btree.New(btreeDegree)
	return true
}

func (lds *LevelDbStorage) Put(key []byte,value []byte,expiration int64) error{
	err:=lds.Db.Put(key,value,nil)
	if err==nil{
		node:=&Node{
			key:key,
			expiration:expiration,
		}
		lds.Index.ReplaceOrInsert(node)
	}
	return err
}

func (lds *LevelDbStorage) Get(key []byte) (value []byte,err error){
	log.Println("unix curr time:",time.Now().Unix())
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

	log.Println(value)
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
}