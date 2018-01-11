package main

import "github.com/syndtr/goleveldb/leveldb"

type DataStorage interface {
	InitDB(dir string) bool
	Put(key []byte,value []byte) error
	Get(key []byte) (value []byte,err error)
	Close()
}

type LevelDbStorage struct{
	Db *leveldb.DB
}

func (lds *LevelDbStorage) InitDB(dir string) bool {
	db,err:=leveldb.OpenFile(dir, nil)
	if err != nil {
		return false
	}
	lds.Db=db
	return true
}

func (lds *LevelDbStorage) Put(key []byte,value []byte) error{
	return lds.Db.Put(key,value,nil)
}

func (lds *LevelDbStorage) Get(key []byte) (value []byte,err error){
	return lds.Db.Get(key,nil)
}

func (lds *LevelDbStorage) Close(){
	lds.Db.Close()
}