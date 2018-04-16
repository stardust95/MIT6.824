package raftkv

import (
	"encoding/gob"
  "mit6.824/src/labrpc"
	"log"
  "mit6.824/src/raft"
	"sync"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	
	return
}

// Op :
type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key 		string
	Value 	string
	Type 		string
}


// RaftKV :
type RaftKV struct {
	mu      sync.RWMutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big
	data		map[string]string
	// Your definitions here.
}


func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.

	if args.Key == "" {
		reply.Err = ErrNoKey
		return
	}
	DPrintf("Get")

	_, ok := kv.rf.GetState()
	if ok {
		kv.mu.RLock()
		reply.Value = kv.data[args.Key]
		kv.mu.RUnlock()
	}else{
		reply.WrongLeader = true
	}
}

func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	if args.Key == ""  {
		reply.Err = ErrNoKey
		return
	}
	DPrintf("PutAppend")

	_, ok := kv.rf.GetState()
	if ok {
		kv.rf.Start(Op{args.Key, args.Value, args.Op})
	}else{
		reply.WrongLeader = true
	}

}

//
// the tester calls Kill() when a RaftKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *RaftKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots with persister.SaveSnapshot(),
// and Raft should save its state (including log) with persister.SaveRaftState().
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *RaftKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(RaftKV)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.data = make(map[string]string)

	// You may need initialization code here.

	go func(){
		for appliedMsg := range kv.applyCh{
			op := appliedMsg.Command.(Op)
			
			kv.mu.Lock()
			if op.Type == OpPut {
				kv.data[op.Key] = op.Value
			}else{
				if val, ok := kv.data[op.Key]; ok {
					kv.data[op.Key] = val + op.Value
				}else{
					kv.data[op.Key] = op.Value
				}
			}
			kv.mu.Unlock()
		}
	}()

	return kv
}
