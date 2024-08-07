package rafter

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
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
	"github.com/sudo-nick16/nord/bitcask"
)

const (
	retainSnapshotCount = 2
	raftTimeout         = 10 * time.Second
)

type Command struct {
	Op    string `json:"op"`
	Key   string `json:"key"`
	Value string `json:"value"`
}

type Rafter struct {
	RaftDir  string
	RaftBind string
	inmem    bool

	mu sync.Mutex
	m  map[string]string
	db *bitcask.Bitcask

	raft   *raft.Raft
	logger *log.Logger
}

func New(inmem bool, db *bitcask.Bitcask, raftdir string, raftbind string) *Rafter {
	return &Rafter{
		RaftDir:  raftdir,
		RaftBind: raftbind,
		inmem:    inmem,

		m:        make(map[string]string, 0),
		db:       db,
		logger:   log.New(os.Stderr, "[rafter] ", log.Ldate|log.Ltime),
	}
}

func (r *Rafter) Open(enableSingle bool, localId string) error {
	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(localId)

	addr, err := net.ResolveTCPAddr("tcp", r.RaftBind)
	if err != nil {
		return err
	}
	transport, err := raft.NewTCPTransport(r.RaftBind, addr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		return err
	}

	snapshots, err := raft.NewFileSnapshotStore(r.RaftDir, retainSnapshotCount, os.Stderr)
	if err != nil {
		return fmt.Errorf("file snapshot error - %+v", err)
	}

	var logStore raft.LogStore
	var stableStore raft.StableStore

	if r.inmem {
		logStore = raft.NewInmemStore()
		stableStore = raft.NewInmemStore()
	} else {
		boltDb, err := raftboltdb.New(raftboltdb.Options{
			Path: filepath.Join(r.RaftDir, "raft.db"),
		})
		if err != nil {
			return fmt.Errorf("new bolt store: %+v", err)
		}
		logStore = boltDb
		stableStore = boltDb
	}

	ra, err := raft.NewRaft(config, (*fsm)(r), logStore, stableStore, snapshots, transport)
	if err != nil {
		return fmt.Errorf("could now create new raft - %+v", err)
	}
	r.raft = ra

	if enableSingle {
		con := raft.Configuration{
			Servers: []raft.Server{
				{
					ID:      config.LocalID,
					Address: transport.LocalAddr(),
				},
			},
		}
		ra.BootstrapCluster(con)
	}
	return nil
}

func (r *Rafter) Get(key []byte) ([]byte, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.db.Get(key)
}

func (r *Rafter) ListKeys() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.db.ListKeys()
}

func (r *Rafter) Put(key, val []byte) error {
	if r.raft.State() != raft.Leader {
		return fmt.Errorf("put key: not leader")
	}
	c := &Command{
		Op:    "put",
		Key:   string(key),
		Value: string(val),
	}
	b, err := json.Marshal(c)
	if err != nil {
		return err
	}
	f := r.raft.Apply(b, raftTimeout)
	return f.Error()
}

func (r *Rafter) Delete(key []byte) error {
	if r.raft.State() != raft.Leader {
		return fmt.Errorf("delete key: not leader")
	}
	c := &Command{
		Op:  "delete",
		Key: string(key),
	}
	b, err := json.Marshal(c)
	if err != nil {
		return err
	}
	f := r.raft.Apply(b, raftTimeout)
	return f.Error()
}

func (r *Rafter) Join(nodeId string, addr string) error {
	r.logger.Printf("received join request for remote node %s at %s", nodeId, addr)

	configFuture := r.raft.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		r.logger.Printf("failed to get raft configuration %+v", err)
		return err
	}

	for _, srv := range configFuture.Configuration().Servers {
		if srv.ID == raft.ServerID(nodeId) || srv.Address == raft.ServerAddress(addr) {
			if srv.Address == raft.ServerAddress(addr) && srv.ID == raft.ServerID(nodeId) {
				r.logger.Printf("node %s at %s already member of cluster, ignoring join request", nodeId, addr)
				return nil
			}
			future := r.raft.RemoveServer(srv.ID, 0, 0)
			if err := future.Error(); err != nil {
				return fmt.Errorf("error removing existing node %s at %s: %s", nodeId, addr, err)
			}
		}
	}

	f := r.raft.AddVoter(raft.ServerID(nodeId), raft.ServerAddress(addr), 0, 0)
	if f.Error() != nil {
		return f.Error()
	}
	r.logger.Printf("node %s at %s joined successfully", nodeId, addr)

	return nil
}

type fsm Rafter

func (f *fsm) Apply(l *raft.Log) interface{} {
	var c Command
	if err := json.Unmarshal(l.Data, &c); err != nil {
		log.Fatalf("failed to unmarshal command: %+v", err)
	}
	switch c.Op {
	case "put":
		{
			f.mu.Lock()
			defer f.mu.Unlock()
			f.db.Put([]byte(c.Key), []byte(c.Value))
		}
	case "delete":
		{
			f.mu.Lock()
			defer f.mu.Unlock()
			f.db.Delete([]byte(c.Key))
		}
	default:
		log.Fatalf("unrecognized command: op: %s", c.Op)
	}
	return nil
}

type fsmSnapshot struct{}

func (f *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	// persist the snapshot
	panic("not implemented")
	return nil
}

func (f *fsmSnapshot) Release() {
	// persist db
	panic("not implemented")
}

func (f *fsm) Snapshot() (raft.FSMSnapshot, error) {
	panic("not implemented")
	f.mu.Lock()
	defer f.mu.Unlock()

	return &fsmSnapshot{}, nil
}

func (f *fsm) Restore(rc io.ReadCloser) error {
	panic("not implemented")
	// restore db from prev state
	return nil
}
