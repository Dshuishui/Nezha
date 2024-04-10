package raft
import (
	"github.com/JasonLou99/Hybrid_KV_Store/util"

	"github.com/syndtr/goleveldb/leveldb"
)

import "sync"

type Persister struct {
	mu        sync.Mutex
	raftstate []byte
	snapshot  []byte
	db *leveldb.DB
}


func (p *Persister) Init(path string) {
	var err error
	// 数据存储路径和一些初始文件
	// 打开指定路径的LevelDB数据库文件，并将返回的leveldb.DB实例赋值给p.db字段，同时将可能的错误赋值给err变量。
	p.db, err = leveldb.OpenFile(path, nil)
	if err != nil {
		util.EPrintf("Open db failed, err: %s", err)
	}
}

func (p *Persister) Put(key string, value string) {
	err := p.db.Put([]byte(key), []byte(value), nil)	// 转换成字节数组是因为LevelDB是只接受字节数组作为键和值的输入。
	if err != nil {
		util.EPrintf("Put key %s value %s failed, err: %s", key, value, err)
	}
}

func (p *Persister) Get(key string) []byte {
	value, err := p.db.Get([]byte(key), nil)
	if err != nil {
		util.EPrintf("Get key %s failed, err: %s", key, err)
		return nil	// 因为有返回值，所有需要return
	}
	return value
}

func MakePersister() *Persister {
	return &Persister{}
}

func (ps *Persister) Copy() *Persister {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	np := MakePersister()
	np.raftstate = ps.raftstate
	np.snapshot = ps.snapshot
	return np
}

func (ps *Persister) SaveRaftState(state []byte) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	ps.raftstate = state
}

func (ps *Persister) ReadRaftState() []byte {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return ps.raftstate
}

func (ps *Persister) RaftStateSize() int {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return len(ps.raftstate)
}

// Save both Raft state and K/V snapshot as a single atomic action,
// to help avoid them getting out of sync.
func (ps *Persister) SaveStateAndSnapshot(state []byte, snapshot []byte) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	ps.raftstate = state
	ps.snapshot = snapshot
}

func (ps *Persister) ReadSnapshot() []byte {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return ps.snapshot
}

func (ps *Persister) SnapshotSize() int {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return len(ps.snapshot)
}