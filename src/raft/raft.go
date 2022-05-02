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

import "sync"
import "sync/atomic"
import "../labrpc"
import "fmt"
import "time"
import "math/rand"

// import "bytes"
// import "../labgob"

// 要求：
// 1.五秒内完成新leader的选举
// 2.心跳间隔大于100ms
// 3.选举超时大于150ms~300ms

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type LogEntry struct {
	Term  int
	Index int
	Msg	  ApplyMsg
}

const (
	Follower string = "follower"
	Candidate string = "canditate"
	Leader string = "leader"
	DefaultElectionTimeout int = 400
	DefaultHeartBeatTimeout int = 150
)
//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	cond      *sync.Cond

	state 		string
	leaderId	int
	lastReceive time.Time

	currentTerm int
	voteFor 	int 
	logs 		[]LogEntry

	commitIndex int
	lastApplied int

	nextIndex 	[]int
	matchIndex 	[]int

	applyCh		chan ApplyMsg
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool

	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = rf.state == Leader
	
	return term, isleader
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
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}


//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

type AppendEntriesArgs struct {
	Term 		 int
	LeaderId 	 int
	PrevLogIndex int
	PrevLogTerm  int
	Logs 		 []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term 	int
	Success bool
}

type RequestVoteArgs struct {
	Term 		 int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term 		int
	VoteGranted bool
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// argLogIndex := args.LastLogIndex
	// argLogTerm := args.LastLogTerm

	rf.mu.Lock() 

	reply.Term = rf.currentTerm
	

	/*
	isLogNew := false
	logLen := len(rf.logs)
	lastLogTerm := rf.logs[logLen - 1].Term
	lastLogIndex := rf.logs[logLen - 1].Index

	if argLogTerm < lastLogTerm || argLogIndex < lastLogIndex && argLogTerm == lastLogTerm {
		isLogNew = true
	}
	*/

	if args.Term > rf.currentTerm || (args.Term == rf.currentTerm && rf.voteFor == -1) {
		rf.currentTerm = args.Term
		rf.voteFor = args.CandidateId
		reply.VoteGranted = true
		rf.lastReceive = time.Now()
		if rf.state == Candidate {
			defer rf.cond.Broadcast()
		}
		rf.state = Follower
		

	} else {
		reply.VoteGranted = false
	}
	rf.mu.Unlock()
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock() 
	
	reply.Term = rf.currentTerm
	// TODO: log replication

	if args.Term < rf.currentTerm {
		reply.Success = false
	} else {
		reply.Success = true
		rf.leaderId = args.LeaderId
		rf.currentTerm = args.Term
		rf.lastReceive = time.Now()
		if rf.state == Candidate {
			defer rf.cond.Broadcast()
		}
		rf.state = Follower
	}
	rf.mu.Unlock()
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

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	if rf.killed() {
		return index, term, isLeader
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isLeader = rf.state == Leader
	go rf.LogReplication()

	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	if rf.killed() == false {
		fmt.Printf("The goroutine hasn't been killed!\n")
	}
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) LogReplication() {

}

/*
 * 作用：心跳
 * 并行发送RPC，告知candidate和follower
 * 情况1：正在发送RPC时收到更大的Term，变回follower。因为是并行发送,可能性很小
 */

func (rf *Raft) HeartBeat() {
	for {
		if rf.killed() {
			return
		}

		rf.mu.Lock()
		me := rf.me
		rf.mu.Unlock()

		for i:=0; i < len(rf.peers); i++ {
			if i == me {
				continue
			}	
			rf.mu.Lock()
			if rf.state != Leader { //情况1
				rf.mu.Unlock()
				return 
			} 
			currentTerm := rf.currentTerm
			rf.mu.Unlock()

		
			go func (x, currentTerm, me int) {
				args := AppendEntriesArgs {
					Term : currentTerm,
					LeaderId : me,
				}
				reply := AppendEntriesReply{}

				DPrintf("AppendEntries RPC begin: %v to %v!\n", me, x)
				ok := rf.sendAppendEntries(x, &args, &reply)
				DPrintf("AppendEntries RPC end: %v to %v!\n", me, x)

				if ok == false {
					DPrintf("AppendEntries RPC return false: %v to %v!\n", rf.me, x)
					return
				}

				rf.mu.Lock()

				if rf.currentTerm < reply.Term {
					rf.state = Follower
					rf.currentTerm = reply.Term
					rf.voteFor = -1
					rf.lastReceive = time.Now() 
				}

				rf.mu.Unlock()

			}(i, currentTerm, me)
		}

		time.Sleep(time.Duration(DefaultHeartBeatTimeout) * time.Millisecond)
	}	
}

/*
 * 作用：领导选举
 * candidate并行发送requestvote RPC，收集选票
 * 如果candidate票数足够，变成leader并发送心跳
 * 否则等待选举超时，开始新一轮的选举
 * 情况1：发送中，收到Leader心跳或更大的Term，变回follower。因为是并行发送,可能性很小
 */

func (rf *Raft) LeaderElection() {
	if rf.killed() {
		return
	}
	
	rf.mu.Lock()
	me := rf.me
	beginTerm := rf.currentTerm //当前的Term
	rf.mu.Unlock()

	VoteCount := 1
	RPCFinish := 1

	for i:=0; i < len(rf.peers); i++ {
		if i == me {
			continue
		}

		rf.mu.Lock()
		currentTerm := rf.currentTerm
		state := rf.state
		rf.mu.Unlock()	

		if state != Candidate { //情况1
			return
		}
		
		go func (x, currentTerm, me int) {
			args := RequestVoteArgs {
				Term : currentTerm,
				CandidateId : me,
			}
			reply := RequestVoteReply{}
			DPrintf("Requestvote RPC begin: %v to %v!\n", me, x)
			startTime := time.Now()
			ok := rf.sendRequestVote(x, &args, &reply)
			durTime := time.Now().Sub(startTime)
			DPrintf("Requestvote RPC end: %v to %v and time is %v!\n", me, x, durTime)
			if ok == false {
				DPrintf("Requestvote RPC return false: %v to %v!\n", me, x)
				return
			}

			rf.mu.Lock()
			// rf.lastReceive = time.Now()
			if rf.currentTerm < reply.Term {
				rf.state = Follower
				rf.currentTerm = reply.Term
			}
			if reply.VoteGranted {
				DPrintf("%v wins the vote from %v!\n", me, x)
				VoteCount++
			}
			RPCFinish++
			rf.mu.Unlock()
			rf.cond.Broadcast()

		}(i, currentTerm, me)
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	for rf.state == Candidate && RPCFinish != len(rf.peers) && 
		VoteCount * 2 <= len(rf.peers) && beginTerm == rf.currentTerm {
		rf.cond.Wait()
	}

	// 当前任期
	if rf.state == Candidate && VoteCount * 2 > len(rf.peers) && beginTerm == rf.currentTerm {
		rf.state = Leader
		rf.leaderId = rf.me
		DPrintf("%v becomes leader in term %v!\n", rf.me, rf.currentTerm)
		go rf.HeartBeat()
	}
	
}

/*
 * 作用：定时检查选举超时
 * 对于超时的follower和candidate将启动新的LeaderElection
 */

func (rf *Raft) CheckElectionTimeout() {
	for {
		rand.Seed(time.Now().UnixNano())
		electionTimeout := DefaultElectionTimeout + rand.Intn(300) // 400ms~700ms

		rf.mu.Lock()
		startTime := time.Now()
		lastTime := rf.lastReceive
		rf.mu.Unlock()		

		if time.Duration(electionTimeout) * time.Millisecond - startTime.Sub(lastTime) > 0{
			time.Sleep(time.Duration(electionTimeout) * time.Millisecond - startTime.Sub(lastTime))
		}
		
		if rf.killed() {
			return
		}
		rf.mu.Lock()
		if rf.state != Leader && rf.lastReceive.Equal(lastTime) {
			rf.state = Candidate
			rf.currentTerm++
			rf.voteFor = rf.me
			rf.lastReceive = time.Now()
			DPrintf("%v election timeout!\n", rf.me)
			go rf.LeaderElection()
		}
		rf.mu.Unlock()

		for {
			rf.mu.Lock()
			state := rf.state
			rf.mu.Unlock()
			if state != Leader {
				break
			}
			time.Sleep(time.Duration(100) * time.Millisecond)
		}
	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	//2A
	rf.voteFor = -1
	rf.leaderId = -1
	rf.state = Follower
	rf.currentTerm = 0
	rf.lastReceive = time.Now()
	rf.cond = sync.NewCond(&rf.mu)

	//2B	
	rf.applyCh = applyCh
	rf.commitIndex = -1
	rf.lastApplied = -1
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	go rf.CheckElectionTimeout()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
