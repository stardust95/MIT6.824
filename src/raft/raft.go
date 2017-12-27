package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"fmt"
	"encoding/gob"
	"bytes"
  "mit6.824/src/labrpc"
  "sync"
  "time"
)

// import "bytes"
// import "encoding/gob"

// State :
type State int

const (
  // StateFollower :
  StateFollower State = iota
  // StateCandidate :
  StateCandidate
  // StateLeader :
  StateLeader
)

const (
  electionTimeout  = 400  // ms
  electionRandomFactor = 100  // ms
  heartbeatTimeout = 150  // ms
  nilIndex         = -1
)

// ApplyMsg :
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
  Index       int
  Command     interface{}
  UseSnapshot bool   // ignore for lab2; only used in lab3
  Snapshot    []byte // ignore for lab2; only used in lab3
}

// LogEntry :
type LogEntry struct {
  Term    int
  Command interface{}
}

// Raft :
// A Go object implementing a single Raft peer.
//
type Raft struct {
  mu        sync.Mutex          // Lock to protect shared access to this peer's state
  peers     []*labrpc.ClientEnd // RPC end points of all peers
  persister *Persister          // Object to hold this peer's persisted state
  me        int                 // this peer's index into peers[]

  // Your data here (2A, 2B, 2C).
  // Look at the paper's Figure 2 for a description of what
  // state a Raft server must maintain.
  currentTerm int
  votedFor    int
  log         []LogEntry
  commitIndex int
  lastApplied int
  nextIndex   []int
  matchIndex  []int

  state 		  State
  isKilled    bool
}

// GetState :
// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

  var term int
  var isleader bool
  // Your code here (2A).
  rf.mu.Lock()
  term = rf.currentTerm
  isleader = (rf.state == StateLeader)
  rf.mu.Unlock()
  return term, isleader
}

func (rf *Raft) print(format string, a ...interface{}) (n int, err error) {
  if Debug > 0 {
    state := ""
    if rf.state == StateLeader {
      state = "Leader"
    }else if rf.state == StateFollower {
      state = "Follower"
    }else{
      state = "Candidate"
    }
    return DPrintf(fmt.Sprintf("Node %d(%s, Term %d): ", rf.me, state, rf.currentTerm) + format, a...)
  }
  return 0, nil
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
  // Your code here (2C).
  // Example:
  // w := new(bytes.Buffer)
  // e := gob.NewEncoder(w)
  // e.Encode(rf.xxx)
  // e.Encode(rf.yyy)
  // data := w.Bytes()
  // rf.persister.SaveRaftState(data)
  buf := new(bytes.Buffer)
  encoder := gob.NewEncoder(buf)

  // rf.mu.Lock()
  encoder.Encode(rf.currentTerm)
  encoder.Encode(rf.votedFor)
  encoder.Encode(rf.log)
  encoder.Encode(rf.isKilled)
  // rf.mu.Unlock()
  
  rf.persister.SaveRaftState(buf.Bytes())
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
  // Your code here (2C).
  // Example:
  // r := bytes.NewBuffer(data)
  // d := gob.NewDecoder(r)
  // d.Decode(&rf.xxx)
  // d.Decode(&rf.yyy)
  if data == nil || len(data) < 1 { // bootstrap without any state?
    return
  }
  buf := bytes.NewBuffer(data)
  decoder := gob.NewDecoder(buf)

  // rf.mu.Lock()
  decoder.Decode(&rf.currentTerm)
  decoder.Decode(&rf.votedFor)
  decoder.Decode(&rf.log)
  decoder.Decode(&rf.isKilled)
  // rf.mu.Unlock()

}

// RequestVoteArgs :
// RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
  // Your data here (2A, 2B).
  Term         int
  CandidateID  int
  LastLogIndex int
  LastLogTerm  int
}

// RequestVoteReply :
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
  // Your data here (2A).
  Term        int
  VoteGranted bool
}

// compareLog :
//	return true if A is older(outdated) than B
func compareLog(lastTermA int, lastIndexA int, lastTermB int, lastIndexB int) bool {
  if lastTermA != lastTermB {
    return lastTermA < lastTermB
  }
  return lastIndexA < lastIndexB
}

// RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
  // Your code here (2A, 2B).
  rf.mu.Lock()
  defer rf.mu.Unlock()

  logLen := len(rf.log)
  lastIndex := logLen - 1
  isCandidateOutdated := compareLog(args.LastLogTerm,
    args.LastLogIndex, rf.log[lastIndex].Term, lastIndex);

  reply.Term = rf.currentTerm

  if rf.currentTerm <= args.Term && 		// if the rpc is not outdated
    !isCandidateOutdated { 	// and if the candidate is not outdated
    rf.state = StateFollower
    rf.currentTerm = args.Term
    reply.VoteGranted = true
    rf.votedFor = args.CandidateID
    rf.persist()

    rf.print("accept vote for Node %d(Term %d), %v", 
      args.CandidateID, args.Term, []int{args.LastLogTerm, args.LastLogIndex, rf.log[lastIndex].Term, lastIndex})
  }else{
    reply.VoteGranted = false
    reason := ""
    if rf.currentTerm > args.Term {
      reason = "RPC term is outdated"
    }else if isCandidateOutdated {
      reason = "Candidate's log is outdated"
    }
    rf.print("refuse to vote for Node %d(Term %d), %s", args.CandidateID, args.Term, reason)
  }
}

// AppendEntries RPC handler.
//
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
  rf.mu.Lock()
  defer rf.mu.Unlock()

  reply.Term = rf.currentTerm

  if rf.currentTerm > args.Term || args.LeaderID == rf.me { // if the rpc is outdated, ignore it
    // rf.print("Node %d(Term %d) ignored append from leader %d(Term %d) after entry {%d, %d}", 
    //   rf.me, rf.currentTerm, args.LeaderID, args.Term, args.PrevLogIndex, args.PrevLogTerm)
    return
  }
  rf.currentTerm = args.Term
  // this node will become follower if it receives a valid heartbeat
  rf.state = StateFollower

  if len(rf.log) <= args.PrevLogIndex || // if the log doesn't contain prevLogIndex
    rf.log[args.PrevLogIndex].Term != args.PrevLogTerm { // if the log entry doesn't match the prev log
    rf.print("refuse to append from leader %d(Term %d) after entry {%d, %d}, log not match", 
      args.LeaderID, args.Term, args.PrevLogIndex, args.PrevLogTerm)
    reply.Success = false
    if args.PrevLogIndex < len(rf.log) {
      conflictTerm := rf.log[args.PrevLogIndex].Term
      i:= args.PrevLogIndex
      for ; i>0; i--{
        if rf.log[i].Term != conflictTerm {
          break
        } 
      }
      reply.ConflictFromIndex = i+1
    }else{
      reply.ConflictFromIndex = len(rf.log)
    }
  }else{
    reply.Success = true

    if len(args.Entries) > 0 {
      rf.log = append(rf.log[:args.PrevLogIndex+1], args.Entries...)
      rf.persist()
    }

    // update commitIndex
    if rf.commitIndex < args.LeaderCommit {   // if 
      if args.LeaderCommit < len(rf.log)-1 {
        rf.commitIndex = args.LeaderCommit
      }else{
        rf.commitIndex = len(rf.log)-1
      }
    }

    if len(args.Entries) > 0 {
      rf.print("accept append %d entries from leader %d(Term %d) from entry {%d, %d}; commited = %d, leaderCommit = %d, logLen = %d, lastTerm = %d", 
        len(args.Entries), args.LeaderID, args.Term, args.PrevLogIndex, args.PrevLogTerm, 
        rf.commitIndex, args.LeaderCommit, len(rf.log), rf.log[len(rf.log)-1].Term)
    }

  }
}

// AppendEntriesArgs :
type AppendEntriesArgs struct {
  Term         int
  LeaderID     int
  PrevLogIndex int
  PrevLogTerm  int
  Entries      []LogEntry
  LeaderCommit int
}

// AppendEntriesReply :
type AppendEntriesReply struct {
  Term              int
  Success           bool
  // ConflictTerm      int
  ConflictFromIndex int
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
  ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
  return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
  ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
  return ok
}

// Start :
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
  rf.mu.Lock()
  defer rf.mu.Unlock()

  if rf.state != StateLeader {
    return nilIndex, nilIndex, false
  }

  // Your code here (2B).

  logLen := len(rf.log)
  index := logLen
  term := rf.currentTerm
  isLeader := true

  thisEntry := LogEntry{rf.currentTerm, command}
  rf.log = append(rf.log, thisEntry)
  rf.matchIndex[rf.me] = len(rf.log)

  rf.persist()

  // rf.print("Client start command %v", command)

  return index, term, isLeader
}

// Kill :
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
  // Your code here, if desired.
  rf.print("is killed")
  // TODO: exit go routines if killed
  rf.isKilled = true
}

// Make :
// The service or tester wants to create a Raft server.
// The ports of all the Raft servers (including this one) are in peers[].
// This server's port is peers[me].
// All the servers' peers[] arrays have the same order.
// Persister is a place for this server to save its persistent state,
//   and also initially holds the most recent saved state, if any.
// applyCh is a channel on which the tester or service expects
//   Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
//   for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
  persister *Persister, applyCh chan ApplyMsg) *Raft {
  rf := &Raft{}
  rf.peers = peers
  rf.persister = persister
  rf.me = me

  // Your initialization code here (2A, 2B, 2C).
  rf.state = StateFollower
  rf.commitIndex = 0
  rf.votedFor = nilIndex
  rf.lastApplied = 0
  rf.currentTerm = 0
  rf.log = []LogEntry{LogEntry{rf.currentTerm, nil}}

  // initialize from state persisted before a crash
  rf.readPersist(persister.ReadRaftState())

  rf.print("Initialize")
  // All servers
  go func() {
    for {
      if rf.isKilled {
        return
      }
      rf.mu.Lock()
      for rf.commitIndex > rf.lastApplied {
        rf.lastApplied++
        applyMsg := ApplyMsg{rf.lastApplied, rf.log[rf.lastApplied].Command, false, nil}
        applyCh <- applyMsg
        rf.print("applied log entry %v", rf.log[rf.lastApplied])
        // Apply rf.log[lastApplied] into its state machine
      }
      rf.mu.Unlock()
      time.Sleep(50 * time.Millisecond)
    }
  }()
  go func() {
    var counterLock sync.Mutex
    for {
      if rf.isKilled {
        return
      }
      rf.mu.Lock()
			if rf.state == StateFollower {  // ONLY follower would have election timeout
				rf.state = StateCandidate
			}
      rf.mu.Unlock()
      duration := time.Duration(electionTimeout +
        Random(-electionRandomFactor, electionRandomFactor))
      time.Sleep(duration * time.Millisecond)
      rf.mu.Lock()

      if rf.state == StateCandidate {
        rf.print("start to request votes for term %d", rf.currentTerm+1)
        counter := 0
        logLen := len(rf.log)
        lastTerm := 0
        lastIndex := logLen-1
        requestTerm := rf.currentTerm+1
        if logLen > 0 {
          lastTerm = rf.log[logLen-1].Term
        }
        rvArgs := RequestVoteArgs{requestTerm, rf.me, lastIndex, lastTerm}
        rvReplies := make([]RequestVoteReply, len(rf.peers))

        for index := range rf.peers {
          go func(index int) {
            ok := rf.sendRequestVote(index, &rvArgs, &rvReplies[index])
            rf.mu.Lock()
            if rvReplies[index].Term > rf.currentTerm {
              rf.currentTerm = rvReplies[index].Term
              rf.state = StateFollower
              rf.persist()
            }else if ok && (rvArgs.Term == rf.currentTerm) && rvReplies[index].VoteGranted {
              counterLock.Lock()
              counter++
              if counter > len(rf.peers)/2 && rf.state != StateLeader {
                rf.state = StateLeader
                rf.currentTerm = requestTerm
                rf.nextIndex = make([]int, len(rf.peers))
                rf.matchIndex = make([]int, len(rf.peers))
                // immediately send heartbeats to others to stop election
                for i := range rf.peers {
                  rf.nextIndex[i] = len(rf.log)
                }
                rf.persist()

                rf.print("become leader for term %d, nextIndex = %v, rvArgs = %v", rf.currentTerm, rf.nextIndex, rvArgs)
              }
              counterLock.Unlock()
            }
            rf.mu.Unlock()
          }(index)
        }
      }
      rf.mu.Unlock()
    }
  }()

  go func(){
    for {
      if rf.isKilled {
        return
      }
      time.Sleep(heartbeatTimeout * time.Millisecond)
      rf.mu.Lock()
      // send AppendEntries(as heartbeats) RPC
      if rf.state == StateLeader {
        currentTerm := rf.currentTerm
        for index := range rf.peers {
          go func(index int) {
            // decrease rf.nextIndex[index] in loop till append success
            for {
              if index == rf.me || rf.state != StateLeader {
                break
              }
              if rf.nextIndex[index] <= 0 || rf.nextIndex[index] > len(rf.log){
                rf.print("Error: rf.nextIndex[%d] = %d, logLen = %d", index, rf.nextIndex[index], len(rf.log))
              }
              rf.mu.Lock()
              appendEntries := rf.log[rf.nextIndex[index]:]
              prevIndex := rf.nextIndex[index]-1
              aeArgs := AppendEntriesArgs{currentTerm, rf.me,
                prevIndex, rf.log[prevIndex].Term,
                appendEntries, rf.commitIndex}
              aeReply := AppendEntriesReply{}
              rf.mu.Unlock()

              ok := rf.sendAppendEntries(index, &aeArgs, &aeReply)
              rf.mu.Lock()
              if ok && rf.currentTerm == aeArgs.Term {  // ensure the reply is not outdated
                if aeReply.Success {
                  rf.matchIndex[index] = len(rf.log)-1
                  rf.nextIndex[index] = len(rf.log)
                  rf.mu.Unlock()
                  break
                }else {
                  if aeReply.Term > rf.currentTerm {  // this leader node is outdated
                    rf.currentTerm = aeReply.Term
                    rf.state = StateFollower
                    rf.persist()
                    rf.mu.Unlock()
                    break
                  }else{                              // prevIndex not match, decrease prevIndex
                    // rf.nextIndex[index]--
                    if aeReply.ConflictFromIndex <= 0 || aeReply.ConflictFromIndex >= len(rf.log){
                      rf.print("Error: aeReply.ConflictFromIndex from %d = %d, logLen = %d", aeReply.ConflictFromIndex, index,  len(rf.log))
                    }
                    rf.nextIndex[index] = aeReply.ConflictFromIndex
                  }
                }
              }
              rf.mu.Unlock()
            }
          }(index)
        }

        // Find logs that has appended to majority and update commitIndex
        for N := rf.commitIndex+1; N<len(rf.log); N++ {
          followerHas := 0
          for index := range rf.peers {
            if rf.matchIndex[index] >= N {
              followerHas++
            }
          }
          // If majority has the log entry of index N
          if followerHas > len(rf.peers) / 2 {
            rf.commitIndex = N
          }
        }
      }
      rf.mu.Unlock()
    }
  }()

  return rf
}



