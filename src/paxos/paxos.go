package paxos

//
// Paxos library, to be included in an application.
// Multiple applications will run, each including
// a Paxos peer.
//
// Manages a sequence of agreed-on values.
// The set of peers is fixed.
// Copes with network failures (partition, msg loss, &c).
// Does not store anything persistently, so cannot handle crash+restart.
//
// The application interface:
//
// px = paxos.Make(peers []string, me string)
// px.Start(seq int, v interface{}) -- start agreement on new instance
// px.Status(seq int) (Fate, v interface{}) -- get info about an instance
// px.Done(seq int) -- ok to forget all instances <= seq
// px.Max() int -- highest instance seq known, or -1
// px.Min() int -- instances before this seq have been forgotten
//

import "net"
import "net/rpc"
import "log"

import "os"
import "syscall"
import "sync"
import "sync/atomic"
import "fmt"
import "math/rand"

// px.Status() return values, indicating
// whether an agreement has been decided,
// or Paxos has not yet reached agreement,
// or it was agreed but forgotten (i.e. < Min()).
type Fate int

const (
	Decided   Fate = iota + 1
	Pending        // not yet decided.
	Forgotten      // decided but forgotten.
)

type PrepareRequestArgs struct {
	seq        int
	proposeNum int
}

type PrepareResponse struct {
	pok     bool
	acceptN int
	acceptV interface{}
}

type AcceptRequestArgs struct {
	seq        int
	proposeNum int
	v          interface{}
}

type AcceptResponse struct {
	aok bool
}

type Paxos struct {
	mu         sync.Mutex
	l          net.Listener
	dead       int32 // for testing
	unreliable int32 // for testing
	rpcCount   int32 // for testing
	peers      []string
	me         int // index into peers[]

	// For Proposer
	// instances    	map[int]interface{}
	proposerNums map[int]int
	max          int
	min          int
	doneOfPeers  map[string]int
	// For Accpetor
	prepareMaxN map[int]int
	acceptN     map[int]int
	acceptV     map[int]interface{}
}

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the replys contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it does not get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please do not change this function.
//
func call(srv string, name string, args interface{}, reply interface{}) bool {
	c, err := rpc.Dial("unix", srv)
	if err != nil {
		err1 := err.(*net.OpError)
		if err1.Err != syscall.ENOENT && err1.Err != syscall.ECONNREFUSED {
			fmt.Printf("paxos Dial() failed: %v\n", err1)
		}
		return false
	}
	defer c.Close()

	err = c.Call(name, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

func (px *Paxos) print(seq int, format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		return DPrintf(fmt.Sprintf("Instance %d(Peer %d): ", seq, px.me)+format, a...)
	}
	return 0, nil
}

// Prepare(K) handler of acceptor
func (px *Paxos) HandlePrepare(args PrepareRequestArgs, resp *PrepareResponse) {
	resp.pok = true
	resp.acceptN = 0
	resp.acceptV = nil
	px.mu.Lock()
	if maxN, exist := px.prepareMaxN[args.seq]; exist {
		if maxN > args.proposeNum {
			resp.pok = false // reject
		} else {
			px.prepareMaxN[args.seq] = args.proposeNum
			if acceptN, ok := px.acceptN[args.seq]; ok {
				resp.acceptN = acceptN
				resp.acceptV = px.acceptV[args.seq]
				// return PrepareResponse{true, acceptN, px.acceptV[args.seq]}
			}
		}
	}
	px.mu.Unlock()
}

// Accept(K, v) handler of acceptor
func (px *Paxos) HandleAccept(args AcceptRequestArgs, resp *AcceptResponse) {
	resp.aok = false
	px.mu.Lock()
	if maxN, exist := px.prepareMaxN[args.seq]; exist {
		if maxN <= args.proposeNum {
			px.prepareMaxN[args.seq] = args.proposeNum
			px.acceptN[args.seq] = args.proposeNum
			px.acceptV[args.seq] = args.v
			resp.aok = true
		}
	}
	px.mu.Unlock()
	// error
	log.Fatal("error")
}

//
// the application wants paxos to start agreement on
// instance seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
//
func (px *Paxos) Start(seq int, v interface{}) {
	// Your code here.

	go func() { // Proposer thread
		isDone := false
		// px.proposerNums[seq] = 0
		proposeNum := 0
		nextTurn := make(chan bool)

		for isDone == false && px.isdead() == false {
			actualProposeNum := (proposeNum * 10) + px.me
			px.print(seq, "start Phase 1: Prepare with proposeNum %d", actualProposeNum)
			var maxNResponse PrepareResponse
			totalResp1 := 0
			pok := 0
			var lockPhase1 sync.RWMutex

			for index, addr := range px.peers {
				go func(idx int, addr string) { // Send request
					reqArgs := PrepareRequestArgs{seq, actualProposeNum}
					response := PrepareResponse{}

					if idx == px.me {
						// call by method
						px.HandlePrepare(reqArgs, &response)
						lockPhase1.Lock()
						totalResp1++
						if response.pok {
							pok++
							if response.acceptV != nil && response.acceptN > maxNResponse.acceptN {
								maxNResponse.acceptN = response.acceptN
								maxNResponse.acceptV = response.acceptV
							}
						}
						lockPhase1.Unlock()
					} else {
						// call by rpc
						ok := call(addr, "Paxos.HandlePrepare", reqArgs, &response)
						lockPhase1.Lock()
						totalResp1++
						if ok && response.pok {
							pok++
							if response.acceptV != nil && response.acceptN > maxNResponse.acceptN {
								maxNResponse.acceptN = response.acceptN
								maxNResponse.acceptV = response.acceptV
							}
						}
						lockPhase1.Unlock()
					}

					lockPhase1.RLock()
					if totalResp1 == len(px.peers) {
						px.print(seq, "received %d pok from %d peers", pok, len(px.peers))
						if pok > len(px.peers)/2 {
							nextTurn <- false
						} else {
							nextTurn <- true
						}
					}
					lockPhase1.RUnlock()
				}(index, addr) // end go func
			} // end for range px.peers

			if <-nextTurn == false { // if a Propose request has been accepted by majority(Prepare finish)
				px.print(seq, "start Phase 2: Accept with proposeNum %d", actualProposeNum)
				aok := 0
				totalResp2 := 0
				var lockPhase2 sync.RWMutex

				for index, addr := range px.peers {
					go func(idx int, addr string) {
						reqArgs := AcceptRequestArgs{seq, actualProposeNum, v}
						response := AcceptResponse{}
						if maxNResponse.acceptV != nil {
							reqArgs.v = maxNResponse.acceptV
						}

						if idx == px.me {
							px.HandleAccept(reqArgs, &response)
							lockPhase2.Lock()
							totalResp2++
							if response.aok {
								aok++
							}
							lockPhase2.Unlock()
						} else {
							ok := call(addr, "Paxos.HandleAccept", reqArgs, &response)
							lockPhase2.Lock()
							totalResp2++
							if ok && response.aok {
								aok++
							}
							lockPhase2.Unlock()
						}

						lockPhase2.RLock()
						if totalResp2 == len(px.peers) {
							px.print(seq, "received %d aok from %d peers", aok, len(px.peers))
							if aok > len(px.peers)/2 {
								nextTurn <- false
							} else {
								nextTurn <- true
							}
						}
						lockPhase2.RUnlock()

					}(index, addr)
				}
			}

			if <-nextTurn == false { //if an Accept request has been accepted by majority
				px.print(seq, "propose accepted")
				isDone = true
			}
			proposeNum++
		}
	}()

}

//
// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
//
func (px *Paxos) Done(seq int) {
	// Your code here.
}

//
// the application wants to know the
// highest instance sequence known to
// this peer.
//
func (px *Paxos) Max() int {
	// Your code here.
	return px.max
}

//
// Min() should return one more than the minimum among z_i,
// where z_i is the highest number ever passed
// to Done() on peer i. A peers z_i is -1 if it has
// never called Done().
//
// Paxos is required to have forgotten all information
// about any instances it knows that are < Min().
// The point is to free up memory in long-running
// Paxos-based servers.
//
// Paxos peers need to exchange their highest Done()
// arguments in order to implement Min(). These
// exchanges can be piggybacked on ordinary Paxos
// agreement protocol messages, so it is OK if one
// peers Min does not reflect another Peers Done()
// until after the next instance is agreed to.
//
// The fact that Min() is defined as a minimum over
// *all* Paxos peers means that Min() cannot increase until
// all peers have been heard from. So if a peer is dead
// or unreachable, other peers Min()s will not increase
// even if all reachable peers call Done. The reason for
// this is that when the unreachable peer comes back to
// life, it will need to catch up on instances that it
// missed -- the other peers therefor cannot forget these
// instances.
//
func (px *Paxos) Min() int {
	// You code here.
	return px.min
}

//
// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
//
func (px *Paxos) Status(seq int) (Fate, interface{}) {
	// Your code here.
	
	return Pending, nil
}

//
// tell the peer to shut itself down.
// for testing.
// please do not change these two functions.
//
func (px *Paxos) Kill() {
	atomic.StoreInt32(&px.dead, 1)
	if px.l != nil {
		px.l.Close()
	}
}

//
// has this peer been asked to shut down?
//
func (px *Paxos) isdead() bool {
	return atomic.LoadInt32(&px.dead) != 0
}

// please do not change these two functions.
func (px *Paxos) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&px.unreliable, 1)
	} else {
		atomic.StoreInt32(&px.unreliable, 0)
	}
}

func (px *Paxos) isunreliable() bool {
	return atomic.LoadInt32(&px.unreliable) != 0
}

//
// the application wants to create a paxos peer.
// the ports of all the paxos peers (including this one)
// are in peers[]. this servers port is peers[me].
//
func Make(peers []string, me int, rpcs *rpc.Server) *Paxos {
	px := &Paxos{}
	px.peers = peers
	px.me = me

	// Your initialization code here.
	// px.instances = make(map[int]interface{})
	px.min = 0
	px.max = 0
	px.doneOfPeers = make(map[string]int)
	px.proposerNums = make(map[int]int)

	px.acceptN = make(map[int]int)
	px.acceptV = make(map[int]interface{})
	px.prepareMaxN = make(map[int]int)

	if rpcs != nil {
		// caller will create socket &c
		rpcs.Register(px)
	} else {
		rpcs = rpc.NewServer()
		rpcs.Register(px)

		// prepare to receive connections from clients.
		// change "unix" to "tcp" to use over a network.
		os.Remove(peers[me]) // only needed for "unix"
		l, e := net.Listen("unix", peers[me])
		if e != nil {
			log.Fatal("listen error: ", e)
		}
		px.l = l

		// please do not change any of the following code,
		// or do anything to subvert it.

		// create a thread to accept RPC connections
		go func() {
			for px.isdead() == false {
				conn, err := px.l.Accept()
				if err == nil && px.isdead() == false {
					if px.isunreliable() && (rand.Int63()%1000) < 100 {
						// discard the request.
						conn.Close()
					} else if px.isunreliable() && (rand.Int63()%1000) < 200 {
						// process the request but force discard of reply.
						c1 := conn.(*net.UnixConn)
						f, _ := c1.File()
						err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
						if err != nil {
							fmt.Printf("shutdown: %v\n", err)
						}
						atomic.AddInt32(&px.rpcCount, 1)
						go rpcs.ServeConn(conn)
					} else {
						atomic.AddInt32(&px.rpcCount, 1)
						go rpcs.ServeConn(conn)
					}
				} else if err == nil {
					conn.Close()
				}
				if err != nil && px.isdead() == false {
					fmt.Printf("Paxos(%v) accept: %v\n", me, err.Error())
				}
			}
		}()
	}

	return px
}
