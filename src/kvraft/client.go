package raftkv

import (
	"sync"
	"mit6.824/src/labrpc"
	"crypto/rand"
	"math/big"
)


type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	mu 			sync.RWMutex
	leader	int
	
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.leader = 0
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	reply := GetReply{}
	args := GetArgs{key, "seqnum"}

	ck.mu.RLock()
	i := ck.leader
	ck.mu.RUnlock()
	for ; ; i = (i+1)%len(ck.servers) {
		ok := ck.servers[i].Call("RaftKV.Get", &args, &reply)
		if ok {
			if reply.Err != "" {
				// handle error
				DPrintf("Error: %s", reply.Err)
			}else if reply.WrongLeader{
				// do nothing
			}else{
				ck.mu.Lock()
				ck.leader = i
				ck.mu.Unlock()
				return reply.Value
			}
		}
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	reply := PutAppendReply{}
	args := PutAppendArgs{key, value, op, "seqnum"}

	ck.mu.RLock()
	i := ck.leader
	ck.mu.RUnlock()
	for ; ; i = (i+1)%len(ck.servers) {
		ok := ck.servers[i].Call("RaftKV.PutAppend", &args, &reply)
		if ok {
			if reply.Err != "" {
				// handle error
				DPrintf("Error: %s", reply.Err)
			}else if reply.WrongLeader {
				continue
			}else{
				ck.mu.Lock()
				ck.leader = i
				ck.mu.Unlock()
				return
			}
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
