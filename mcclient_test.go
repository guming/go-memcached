package main

import (
	"fmt"
	"testing"
	"memcached/memcached-client"
	"github.com/google/btree"
	"flag"
	"bytes"
	"time"
)

func TestClient_Get_get(t *testing.T) {
	mc := memcached_client.New("192.168.7.133:11211")
	mc.Set(&memcached_client.Item{Key: "foo", Value: []byte("my value simple3"),Flags:32,Expiration:60})
	time.Sleep(10*time.Millisecond)
	fmt.Println("-----")
	it, err := mc.Get("foo")
	if err!=nil||it==nil{
		fmt.Println(err)
	}else {
		fmt.Println("key:" + it.Key + " value: " + string(it.Value))
	}

}
var btreeDegree1 = flag.Int("degree", 32, "B-Tree degree")

type Node1 struct{
	expiration int32
	key []byte
}

func (n *Node1) Less(than btree.Item) bool{
	return bytes.Compare(n.key,than.(*Node1).key)==-1
}

func TestLevelDbStorage_Get(t *testing.T) {
	tr := btree.New(*btreeDegree1)
	const treeSize = 10000
	node:=&Node1{
		key:[]byte("foo"),
		expiration:3600,
	}
	tr.ReplaceOrInsert(node)
	node0:=&Node1{
		key:[]byte("foo"),
	}
	ni:=tr.Get(node0)
	fmt.Println(string(ni.(*Node1).key))
	node1:=&Node1{
		key:[]byte("fo"),
		expiration:3500,
	}
	tr.ReplaceOrInsert(node1)
	var revs []*Node1
	tr.AscendGreaterOrEqual(node1, func(i btree.Item) bool {
		if i.Less(node1) {
			return false
		}
		curNode := i.(*Node1)
		revs = append(revs, curNode)
		return true
	})
	for n,value :=range revs{
		fmt.Println(n,value.expiration)
	}
}
