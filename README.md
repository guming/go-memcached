# go-memcached
* memcached ascii protocol(set/get)
* support storage
* dependences etcd,btree,leveldb
* raft cluster



### test
#### 1.go build
#### 2.start three process
*  ./go-memcached -h 192.168.7.133 -cluster=http://192.168.7.133:11213,http://192.168.7.139:11213,http://192.168.7.136:11213 -port 11214 -id 1
*  ./go-memcached -h 192.168.7.139 -cluster=http://192.168.7.133:11213,http://192.168.7.139:11213,http://192.168.7.136:11213 -port 11214 -id 2
*  ./go-memcached -h 192.168.7.136 -cluster=http://192.168.7.133:11213,http://192.168.7.139:11213,http://192.168.7.136:11213 -port 11214 -id 3

* for i in {1..10}; do echo "set t_$i 0 0 4\r\n1002\r\n"|nc 192.168.7.133 11211;done;
* for i in {1..10}; do echo "get t_$i\r\n"|nc 192.168.7.139 11211;done;