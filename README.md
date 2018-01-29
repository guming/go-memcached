# go-memcached
* memcached ascii protocol(set/get)
* support storage
* dependences etcd/raft,btree,leveldb
* HA:raft cluster or master/slave or single node(default)

###
* 兼容memcached协议的kv存储，支持ttl
* 高性能 5w/s 读写支持
* 高可用 支持raft集群和主从结构 
* 数据落地ssd 磁盘,接触内存限制
* 适用场景 
    >1. 元数据存储
        meta storage + data storage 可以组成一个支持分片的分布式kv storage
    >2. 缓存落地存储

### test
#### 1.go build
#### 2.start three nodes
*  ./go-memcached -h 192.168.7.133 -cluster=http://192.168.7.133:11213,http://192.168.7.139:11213,http://192.168.7.136:11213 -port 11214 -id 1
*  ./go-memcached -h 192.168.7.139 -cluster=http://192.168.7.133:11213,http://192.168.7.139:11213,http://192.168.7.136:11213 -port 11214 -id 2
*  ./go-memcached -h 192.168.7.136 -cluster=http://192.168.7.133:11213,http://192.168.7.139:11213,http://192.168.7.136:11213 -port 11214 -id 3

* for i in {1..10}; do echo "set t_$i 0 0 4\r\n1002\r\n"|nc 192.168.7.133 11211;done;
* for i in {1..10}; do echo "get t_$i\r\n"|nc 192.168.7.139 11211;done;
#### 3.master/slave using binlog without raft
##### 本机启动
*  ./go-memcached -mode master -p 11211
*  ./go-memcached -mode slave -dir /path/to/ldb -p 11212

#### 4.benchmark(single node)
##### go test benchmark_test.go -bench=. -run=none -test.benchtime 1s
* Benchmark_ParallelPut-8   	   50000	     24594 ns/op
* Benchmark_ParallelGet-8   	  100000	     20339 ns/op
* PASS
* ok  	command-line-arguments	3.962s

### TODO


#### add ascii protocol commands:delete
#### complete the unit testing

