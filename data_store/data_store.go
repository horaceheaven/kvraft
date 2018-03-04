package datastore

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
)

const (
	retainSnapshotCount = 2
	raftTimeout         = 10 * time.Second
)

type command struct {
	Op    string `json:"op,omitempty"`
	Key   string `json:"key,omitempty"`
	Value string `json:"value,omitempty"`
}

type Store struct {
	RaftDir  string
	RaftBind string

	mu        sync.Mutex
	dataStore map[string]string

	raft *raft.Raft

	logger *log.Logger
}

func New() *Store {
	return &Store{
		dataStore: make(map[string]string),
		logger:    log.New(os.Stderr, "[data_store] ", log.LstdFlags),
	}
}

func (store *Store) Open(enableSinlge bool, localID string) error {
	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(localID)

	addr, err := net.ResolveTCPAddr("tcp", store.RaftBind)
	if err != nil {
		return err
	}

	transport, err := raft.NewTCPTransport(store.RaftBind, addr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		return err
	}

	snapshots, err := raft.NewFileSnapshotStore(store.RaftDir, retainSnapshotCount, os.Stderr)
	if err != nil {
		return fmt.Errorf("%s", err)
	}

	logStore, err := raftboltdb.NewBoltStore(filepath.Join(store.RaftDir, "raft.db"))
	if err != nil {
		return fmt.Errorf("%s", err)
	}

	raftHandle, err := raft.NewRaft(config, (*fsm)(store), logStore, logStore, snapshots, transport)
	if err != nil {
		return fmt.Errorf("%s", err)
	}

	store.raft = raftHandle

	if enableSinlge {
		configuration := raft.Configuration{
			Servers: []raft.Server{
				{
					ID:      config.LocalID,
					Address: transport.LocalAddr(),
				},
			},
		}
		raftHandle.BootstrapCluster(configuration)
	}

	return nil
}

func (store *Store) Get(key string) (string, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	return store.dataStore[key], nil
}

func (store *Store) Set(key, value string) error {
	if store.raft.State() != raft.Leader {
		return fmt.Errorf("not leader")
	}

	cmd := &command{
		Op:    "set",
		Key:   key,
		Value: value,
	}

	object, err := json.Marshal(cmd)
	if err != nil {
		return err
	}

	apply := store.raft.Apply(object, raftTimeout)
	return apply.Error()
}

func (store *Store) Delete(key string) error {
	if store.raft.State() != raft.Leader {
		return fmt.Errorf("not leader")
	}

	cmd := &command{
		Op:  "delete",
		Key: key,
	}

	object, err := json.Marshal(cmd)
	if err != nil {
		return err
	}

	apply := store.raft.Apply(object, raftTimeout)
	return apply.Error()
}

func (store *Store) Join(nodeID, addr string) error {
	store.logger.Printf("got join request from remote node %s at %s", nodeID, addr)

	addVoter := store.raft.AddVoter(raft.ServerID(nodeID), raft.ServerAddress(addr), 0, 0)
	if addVoter.Error() != nil {
		return addVoter.Error()
	}

	store.logger.Printf("node %s at %s joined successfully", nodeID, addr)
	return nil
}

type fsm Store

func (f *fsm) Apply(log *raft.Log) interface{} {
	var cmd command
	if err := json.Unmarshal(log.Data, &cmd); err != nil {
		panic(fmt.Sprintf("failed to unmarshal command: %s", err.Error()))
	}

	// switch cmd.Op {
	// case "set":
	// 	return f.ap
	// }
	return nil // stub
}

func (f *fsm) Snapshot() (raft.FSMSnapshot, error) {
	return nil, nil
}

func (f *fsm) Restore(rc io.ReadCloser) error {
	return nil
}

type fsmSnapshot struct {
	store map[string]string
}

func (f *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	err := func() error {
		object, err := json.Marshal(f.store)
		if err != nil {
			return err
		}

		if _, err := sink.Write(object); err != nil {
			return err
		}

		return sink.Close()
	}()

	if err != nil {
		sink.Cancel()
	}

	return err
}

func (f *fsmSnapshot) Release() {}

func (f *fsm) applySet(key, value string) interface{} {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.dataStore[key] = value
	return nil
}

func (f *fsm) applyDelete(key string) interface{} {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.dataStore, key)
	return nil
}
