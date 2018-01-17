package main

import (
	"github.com/coreos/etcd/raft/raftpb"
	"strings"
	"sync"
	"encoding/gob"
	"bytes"
	"log"
	"strconv"
	"net/http"
	"io/ioutil"
	"net/url"
	"github.com/coreos/etcd/raft"
	"github.com/coreos/etcd/rafthttp"
	"time"
	"net"
	"errors"
	"github.com/coreos/etcd/etcdserver/stats"
	"github.com/coreos/etcd/pkg/types"
	"context"
)


type kv struct {
	Key []byte
	Val []byte
	Exiration int64
}

type RaftServer struct{
	storage DataStorage
}

func (raft *RaftServer)StartRaft(id int,cluster string,join bool,kvport int) *RaftKVStore{
	proposeC := make(chan []byte)
	defer close(proposeC)
	confChangeC := make(chan raftpb.ConfChange)
	defer close(confChangeC)
	commitC,errorC:=newRaftNode(id,strings.Split(cluster,","),join,proposeC,confChangeC)
	kvstore :=&RaftKVStore{proposeC:proposeC,KvStore:raft.storage}
	go kvstore.readCommits(commitC, errorC)
	log.Println("goroutine readcommit")
	//runStateToKVStore(proposeC, commitC,KVStoreDB,errorC)
	ServeHttpKVAPI(kvport, confChangeC, errorC)
	return kvstore
}

type RaftKVStore struct{
	proposeC    chan<- []byte
	mu          sync.RWMutex
	KvStore     DataStorage

}

func (s *RaftKVStore) readCommits(commitC <-chan *[]byte, errorC <-chan error) {
	for data := range commitC {
		if data == nil {
			continue
		}

		var dataKv kv
		dec := gob.NewDecoder(bytes.NewBufferString(string(*data)))
		if err := dec.Decode(&dataKv); err != nil {
			log.Fatalf("rafte error: could not decode message (%v)", err)
		}
		s.mu.Lock()
		s.KvStore.Put(dataKv.Key,dataKv.Val,dataKv.Exiration)
		s.mu.Unlock()
	}
	if err, ok := <-errorC; ok {
		log.Fatal(err)
	}
}

func (s *RaftKVStore) Get(key []byte) ([]byte, error) {
	v, err := s.KvStore.Get(key)
	return v, err
}

func (s *RaftKVStore) Put(k []byte, v []byte,expiration int64) error{
	log.Println("put ...")
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(kv{k, v,expiration}); err != nil {
		log.Fatal(err)
		return err
	}
	s.proposeC <- buf.Bytes()
	return nil
}

func (s *RaftKVStore) Close(){
	s.KvStore.Close()
}


type httpKVAPI struct {
	confChangeC chan<- raftpb.ConfChange
}

func (h *httpKVAPI) ServeHTTP(w http.ResponseWriter,r *http.Request){
	key := r.RequestURI
	switch {
	case r.Method == "POST":
		//add new member
		url, err := ioutil.ReadAll(r.Body)
		if err != nil {
			log.Printf("Failed to read on POST (%v)\n", err)
			http.Error(w, "Failed on POST", http.StatusBadRequest)
			return
		}

		nodeId, err := strconv.ParseUint(key[1:], 0, 64)
		if err != nil {
			log.Printf("Failed to convert ID for conf change (%v)\n", err)
			http.Error(w, "Failed on POST", http.StatusBadRequest)
			return
		}

		cc := raftpb.ConfChange{
			Type:    raftpb.ConfChangeAddNode,
			NodeID:  nodeId,
			Context: url,
		}
		h.confChangeC <- cc

		w.WriteHeader(http.StatusNoContent)
	case r.Method == "DELETE":
		nodeId, err := strconv.ParseUint(key[1:], 0, 64)
		if err != nil {
			log.Printf("Failed to convert ID for conf change (%v)\n", err)
			http.Error(w, "Failed on DELETE", http.StatusBadRequest)
			return
		}

		cc := raftpb.ConfChange{
			Type:   raftpb.ConfChangeRemoveNode,
			NodeID: nodeId,
		}
		h.confChangeC <- cc
		w.WriteHeader(http.StatusNoContent)
	default:
		w.Header().Set("Allow", "PUT")
		w.Header().Add("Allow", "GET")
		w.Header().Add("Allow", "POST")
		w.Header().Add("Allow", "DELETE")
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func ServeHttpKVAPI(port int, confChangeC chan<- raftpb.ConfChange, errorC <-chan error) {
	log.Println("ServeHttpKVAPI ",port)
	srv := http.Server{
		Addr: ":" + strconv.Itoa(port),
		Handler: &httpKVAPI{
			confChangeC: confChangeC,
		},
	}
	log.Println("end....")
	go func() {
		if err := srv.ListenAndServe(); err != nil {
			log.Println(err)
		}
	}()

	if err, ok := <-errorC; ok {
		log.Println(err)
	}

}



type raftNode struct {
	proposeC <-chan []byte
	confChange <-chan raftpb.ConfChange
	confState     raftpb.ConfState
	commitC     chan<- *[]byte
	errorC      chan<- error
	id          int
	peers []string
	join bool
	lastIndex   uint64
	appliedIndex  uint64
	node        raft.Node
	raftStorage *raft.MemoryStorage
	transport *rafthttp.Transport
	stopc     chan struct{}
	httpstopc chan struct{}
	httpdonec chan struct{}
}

func newRaftNode (id int, peers []string, join bool, proposeC <-chan []byte,
	confChange <-chan raftpb.ConfChange) (<-chan *[]byte, <-chan error) {
	commitC := make(chan *[]byte)
	errorC := make(chan error)


	rc:=&raftNode{
		proposeC:    proposeC,
		confChange: confChange,
		commitC:     commitC,
		errorC:      errorC,
		id:          id,
		peers:       peers,
		join:        join,
		stopc:       make(chan struct{}),
		httpstopc:   make(chan struct{}),
		httpdonec:   make(chan struct{}),
	}
	go rc.startRaft()
	return commitC, errorC

}

func (rc *raftNode) startRaft(){
	rc.raftStorage = raft.NewMemoryStorage()
	rc.commitC <- nil
	rpeers := make([]raft.Peer, len(rc.peers))
	for i := range rpeers {
		rpeers[i] = raft.Peer{ID: uint64(i + 1)}
	}
	c := &raft.Config{
		ID:              uint64(rc.id),
		ElectionTick:    10,
		HeartbeatTick:   1,
		Storage:         rc.raftStorage,
		MaxSizePerMsg:   1024 * 1024,
		MaxInflightMsgs: 256,
	}
	startPeers := rpeers
	if rc.join {
		startPeers = nil
	}
	rc.node = raft.StartNode(c, startPeers)
	//stats server init
	ss := stats.NewServerStats("","")
	rc.transport = &rafthttp.Transport{
		ID:          types.ID(rc.id),
		ClusterID:   0x1000,
		Raft:        rc,
		ServerStats: ss,
		LeaderStats: stats.NewLeaderStats(strconv.Itoa(rc.id)),
		ErrorC:      make(chan error),
	}
	rc.transport.Start()
	for i := range rc.peers {
		if i+1 != rc.id {
			rc.transport.AddPeer(types.ID(i+1), []string{rc.peers[i]})
		}
	}
	go rc.serveRaft()
	go rc.serveChannels()
}

func (rc *raftNode) stop() {
	rc.stopHTTP()
	close(rc.commitC)
	close(rc.errorC)
	rc.node.Stop()
}



func (rc *raftNode) entriesToApply(ents []raftpb.Entry) (nents []raftpb.Entry) {
	if len(ents) == 0 {
		return
	}
	firstIdx := ents[0].Index
	if firstIdx > rc.appliedIndex+1 {
		log.Fatalf("first index of committed entry[%d] should <= progress.appliedIndex[%d] 1", firstIdx, rc.appliedIndex)
	}
	if rc.appliedIndex-firstIdx+1 < uint64(len(ents)) {
		nents = ents[rc.appliedIndex-firstIdx+1:]
	}
	return nents
}

func (rc *raftNode) publishEntries(ents []raftpb.Entry) bool {
	for i := range ents {
		switch ents[i].Type {
		case raftpb.EntryNormal:
			if len(ents[i].Data) == 0 {
				break
			}
			s := ents[i].Data
			//todo commit

			select {
			case rc.commitC <- &s:
			case <-rc.stopc:
				return false
			}

		case raftpb.EntryConfChange:
			var cc raftpb.ConfChange
			cc.Unmarshal(ents[i].Data)
			rc.confState = *rc.node.ApplyConfChange(cc)
			switch cc.Type {
			case raftpb.ConfChangeAddNode:
				if len(cc.Context) > 0 {
					rc.transport.AddPeer(types.ID(cc.NodeID), []string{string(cc.Context)})
				}
			case raftpb.ConfChangeRemoveNode:
				if cc.NodeID == uint64(rc.id) {
					log.Println("I've been removed from the cluster! Shutting down.")
					return false
				}
				rc.transport.RemovePeer(types.ID(cc.NodeID))
			}
		}

		// after commit, update appliedIndex
		rc.appliedIndex = ents[i].Index

		// special nil commit to signal replay has finished
		if ents[i].Index == rc.lastIndex {
			select {
			case rc.commitC <- nil:
			case <-rc.stopc:
				return false
			}
		}
	}
	return true
}

func (rc *raftNode) stopHTTP() {
	rc.transport.Stop()
	close(rc.httpstopc)
	<-rc.httpdonec
}

func (rc *raftNode) Process(ctx context.Context, m raftpb.Message) error {
	return rc.node.Step(ctx, m)
}
func (rc *raftNode) IsIDRemoved(id uint64) bool { return false }
func (rc *raftNode) ReportUnreachable(id uint64) {}
func (rc *raftNode) ReportSnapshot(id uint64, status raft.SnapshotStatus) {}



func (rc *raftNode) serveChannels() {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	go func() {
		var confChangeCount uint64 = 0

		for rc.proposeC != nil && rc.confChange != nil {
			select {
			case prop, ok := <-rc.proposeC:
				if !ok {
					rc.proposeC = nil
				} else {
					// blocks until accepted by raft state machine
					rc.node.Propose(context.TODO(), []byte(prop))
				}

			case cc, ok := <-rc.confChange:
				if !ok {
					rc.confChange = nil
				} else {
					confChangeCount += 1
					cc.ID = confChangeCount
					rc.node.ProposeConfChange(context.TODO(), cc)
				}
			}
		}
		// client closed channel; shutdown raft if not already
		close(rc.stopc)
	}()

	// event loop on raft state machine updates
	for {
		select {
		case <-ticker.C:
			rc.node.Tick()

		case rd := <-rc.node.Ready():
			rc.raftStorage.Append(rd.Entries)
			rc.transport.Send(rd.Messages)
			if ok := rc.publishEntries(rc.entriesToApply(rd.CommittedEntries)); !ok {
				rc.stop()
				return
			}
			rc.node.Advance()

		case err := <-rc.transport.ErrorC:
			rc.writeError(err)
			return

		case <-rc.stopc:
			rc.stop()
			return
		}
	}
}
func (rc *raftNode) serveRaft() {
	url, err := url.Parse(rc.peers[rc.id-1])
	if err != nil {
		log.Fatalf("raft: Failed parsing URL (%v)", err)
	}

	ln, err := NewStoppableListener(url.Host, rc.httpstopc)
	if err != nil {
		log.Fatalf("raft: Failed to listen rafthttp (%v)", err)
	}

	err = (&http.Server{Handler: rc.transport.Handler()}).Serve(ln)
	select {
	case <-rc.httpstopc:
	default:
		log.Fatalf("raft: Failed to serve rafthttp (%v)", err)
	}
	close(rc.httpdonec)
}

func (rc *raftNode) writeError(err error) {
	rc.stopHTTP()
	close(rc.commitC)
	rc.errorC <- err
	close(rc.errorC)
	rc.node.Stop()
}

type stoppableListener struct {
	*net.TCPListener
	stopc <-chan struct{}
}

func NewStoppableListener(addr string, stopc <-chan struct{}) (*stoppableListener, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	return &stoppableListener{ln.(*net.TCPListener), stopc}, nil
}

func (ln stoppableListener) Accept() (c net.Conn, err error) {
	connc := make(chan *net.TCPConn, 1)
	errc := make(chan error, 1)
	go func() {
		tc, err := ln.AcceptTCP()
		if err != nil {
			errc <- err
			return
		}
		connc <- tc
	}()
	select {
	case <-ln.stopc:
		return nil, errors.New("server stopped")
	case err := <-errc:
		return nil, err
	case tc := <-connc:
		tc.SetKeepAlive(true)
		tc.SetKeepAlivePeriod(3 * time.Minute)
		return tc, nil
	}
}

