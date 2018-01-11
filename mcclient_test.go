package main

import (
	"fmt"
	"testing"
	"memcached/memcached-client"
)

func TestClient_Get_get(t *testing.T) {
	mc := memcached_client.New("127.0.0.1:11211")
	//mc.Set(&memcached_client.Item{Key: "foo", Value: []byte("my value")})

	fmt.Println("-----")
	it, err := mc.Get("foo")
	if err!=nil||it==nil{
		fmt.Println(err)
	}else {
		fmt.Println("key:" + it.Key + " value: " + string(it.Value))
	}
}
