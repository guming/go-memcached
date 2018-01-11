package main

import (
	"memcached/memcached-client"
	"fmt"
)

func main() {
	mc := memcached_client.New("127.0.0.1:11211")
	mc.Set(&memcached_client.Item{Key: "foo", Value: []byte("my value")})


	it, err := mc.Get("foo")
	if err!=nil{
		fmt.Println(err)
	}
	fmt.Println("key:"+it.Key+" value: "+string(it.Value))
}
