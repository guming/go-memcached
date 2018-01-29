package main

import (
	"testing"
	"time"
	"memcached/client"
	"strconv"
	//"fmt"
)

func Benchmark_ParallelPut(b *testing.B) {
	mc := client.New("127.0.0.1:11211")
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i:=0
		for pb.Next() {
			i++
			//fmt.Println("put foo"+strconv.Itoa(i))
			mc.Set(&client.Item{Key: "foo"+strconv.Itoa(i), Value: []byte("my value simple foo"+strconv.Itoa(i)),Flags:32,Expiration:3600})
		}
	})
}

func Benchmark_ParallelGet(b *testing.B) {
	mc := client.New("127.0.0.1:11211")
	//mc.Set(&client.Item{Key: "foo3", Value: []byte("my value simple 127-1"),Flags:32,Expiration:3600})
	time.Sleep(10*time.Millisecond)
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		i:=0
		//f_count:=0
		for pb.Next() {
			i++
			if i>5000{
				i=5000
			}
			key:="foo"+strconv.Itoa(i)
			mc.Get(key)

		}
	})

}