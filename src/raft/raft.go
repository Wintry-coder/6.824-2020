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
	Msg   ApplyMsg
}

type LogState struct {
	Commit  	 bool
	SuccessCount int
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
	
	//2A
	state 		string
	lastReceive time.Time
	currentTerm int
	voteFor 	int    // initialized to -1
	voteGranted []int  // initialized to -1

	//2B
	logs 		[]LogEntry
	logstates	[]LogState
	commitIndex int		// initialized to 0
	lastApplied int		// initialized to 0
	nextIndex 	[]int	// initialized to 1
	matchIndex 	[]int	// initialized to 0
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
	PrevLogIndex int
	PrevLogTerm  int
	Logs 		 []LogEntry
	LeaderCommit int
	IsHeartBeat  bool
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

/*
 * 只有成功投票才会重置定时器
 * 防止拥有过期日志的candidate反复超时，而拥有新日志的follower不超时
 */
func (rf *Raft) RequestVoteRPC(args *RequestVoteArgs, reply *RequestVoteReply) {
	argLogIndex := args.LastLogIndex
	argLogTerm := args.LastLogTerm

	rf.mu.Lock() 

	reply.Term = rf.currentTerm
	isRequestLogNew := true

	lastLogTerm := rf.logs[len(rf.logs) - 1].Term
	lastLogIndex := rf.logs[len(rf.logs) - 1].Index
	if argLogTerm < lastLogTerm || argLogIndex < lastLogIndex && argLogTerm == lastLogTerm {
		isRequestLogNew = false
	}
	
	isAcceptRequest := isRequestLogNew && (args.Term > rf.currentTerm || (args.Term == rf.currentTerm && rf.voteFor == -1))

	if isAcceptRequest {
		rf.currentTerm = args.Term
		rf.voteFor = args.CandidateId
		reply.VoteGranted = true
		rf.lastReceive = time.Now()
		rf.state = Follower
	} else {
		reply.VoteGranted = false
	}
	rf.mu.Unlock()
}

/*
 * Term >= currentTerm 就会重置定时器，因为本来就有心跳包的作用
 */
func (rf *Raft) AppendEntriesRPC(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock() 
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm

	isLogMatch := false

	if len(rf.logs) - 1 >= args.PrevLogIndex {
		isLogMatch = rf.logs[args.PrevLogIndex].Term == args.PrevLogTerm
	}
	
	if args.Term < rf.currentTerm {
		reply.Success = false
	} else {
		if args.Term > rf.currentTerm {
			rf.voteFor = -1
		}
		rf.lastReceive = time.Now()
		rf.currentTerm = args.Term
		rf.state = Follower

		if isLogMatch {
			reply.Success = true
			if !args.IsHeartBeat {
				newLog := args.Logs[0]
				newLogState := LogState{}
				if len(rf.logs) > args.PrevLogIndex + 1 {
					rf.logs[args.PrevLogIndex + 1] = newLog 
					rf.logstates[args.PrevLogIndex + 1] = newLogState
				} else {
					rf.logs = append(rf.logs, newLog)
					rf.logstates = append(rf.logstates, newLogState)
					DPrintf("%v add log[%v]\n", rf.me, args.PrevLogIndex + 1)
				}
			}
			if args.LeaderCommit > rf.commitIndex {
				rf.commitIndex = args.LeaderCommit
				if rf.commitIndex > len(rf.logs) - 1 {
					rf.commitIndex = len(rf.logs) - 1
				}
				for rf.lastApplied < rf.commitIndex {
					rf.lastApplied++
					rf.applyCh <- rf.logs[rf.lastApplied].Msg
					DPrintf("%v applys log %v!\n", rf.me, rf.lastApplied)
				}
			}
		} else {
			reply.Success = false
		}	
	}
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
	ok := rf.peers[server].Call("Raft.RequestVoteRPC", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntriesRPC", args, reply)
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
	if isLeader {
		index = len(rf.logs)
		msg := ApplyMsg {
			CommandValid : true, 
			Command : command,
			CommandIndex : index,
		}
		newLog := LogEntry {
			Term : term,
			Index : index,
			Msg : msg,
		}
		newLogState := LogState {
			Commit : false,
			SuccessCount : 1,
		}
		rf.logs = append(rf.logs, newLog)
		rf.logstates = append(rf.logstates, newLogState)
		DPrintf("Leader %v add log %v\n", rf.me, index)
	}

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

/*
 * 作用：心跳
 * 并行发送RPC，告知candidate和follower
 */

func (rf *Raft) AppendEntries(x int) {
	for {
		if rf.killed() {
			DPrintf("%v has been killed\n", rf.me)
			return
		}

		rf.mu.Lock()
		isLeader := rf.state == Leader
		if !isLeader {
			rf.mu.Unlock()	
			return
		}	
		currentTerm := rf.currentTerm
		nextIndex := rf.nextIndex[x]
		prevLogIndex := nextIndex - 1
		prevLogTerm := rf.logs[prevLogIndex].Term
		leaderCommit := rf.commitIndex

		// 在每次只发一个log的情况下，leaderCommit需要限制，否则可能导致错误提交。
		// 比如follower后面的log都是错误的，而当前只重写了一个log。
		// 很大的leaderCommit会导致follower错误提交后续的log。
		// 所以leaderCommit最大只能是当前log的index值，用于让follower复制完成后立马提交该log。
		// TODO:
		if leaderCommit > nextIndex {
			leaderCommit = nextIndex
		}

		args := AppendEntriesArgs {
			Term : currentTerm,
			PrevLogIndex : prevLogIndex,
			PrevLogTerm : prevLogTerm, 
			LeaderCommit : leaderCommit,
		}
		args.Logs = []LogEntry{}
		if len(rf.logs) == nextIndex {
			args.IsHeartBeat = true
		} else {
			args.IsHeartBeat = false
			args.Logs = append(args.Logs, rf.logs[nextIndex])
		}
		reply := AppendEntriesReply{}
		rf.mu.Unlock()	

		if args.IsHeartBeat {
			DPrintf("HeartBeat begin: %v send (%v %v %v) to %v!\n", rf.me, prevLogIndex, prevLogTerm, leaderCommit, x)
		} else {
			DPrintf("AppendEntries begin: %v send (%v %v %v) and log[%v] to %v!\n", rf.me, prevLogIndex, prevLogTerm, leaderCommit, prevLogIndex + 1, x)
		}
		
		ok := rf.sendAppendEntries(x, &args, &reply)

		if args.IsHeartBeat {
			DPrintf("HeartBeat end: %v send (%v %v %v) to %v!\n", rf.me, prevLogIndex, prevLogTerm, leaderCommit, x)
		} else {
			DPrintf("AppendEntries end: %v send (%v %v %v) and log[%v] to %v!\n", rf.me, prevLogIndex, prevLogTerm, leaderCommit, prevLogIndex + 1, x)
		}

		if rf.killed() {
			return
		}
		if !ok {
			DPrintf("AppendEntriesRPC return false!\n")
			return
		}

		rf.mu.Lock()
		defer rf.mu.Unlock()	

		isLeader = rf.state == Leader
		if !isLeader || currentTerm != rf.currentTerm {
			DPrintf("%v is not leader or term changed\n", rf.me)
			return
		}		

		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.state = Follower
			rf.voteFor = -1
			rf.lastReceive = time.Now() 
			DPrintf("%v become follower\n", rf.me)
			return
		} 

		if reply.Success {
			DPrintf("Success! leader %v to %v\n", rf.me, x)
			if !args.IsHeartBeat {
				rf.matchIndex[x] = rf.nextIndex[x]
				rf.nextIndex[x]++
				successLogIndex := rf.matchIndex[x]
				if rf.logs[successLogIndex].Term == rf.currentTerm {
					//当前Term的log可以直接根据计数提交，无视乱序接受RPC响应
					rf.logstates[successLogIndex].SuccessCount++
					if rf.logstates[successLogIndex].SuccessCount * 2 > len(rf.peers) {
						rf.logstates[successLogIndex].Commit = true
						rf.commitIndex = rf.logs[successLogIndex].Index
						//循环应用之前未应用的log，旧的Term在此处间接提交
						for rf.lastApplied < rf.commitIndex {
							rf.lastApplied++
							rf.applyCh <- rf.logs[rf.lastApplied].Msg
							DPrintf("Leader %v applys log %v !\n", rf.me, rf.lastApplied)
						}
					}
				}

			}
			if len(rf.logs) == rf.nextIndex[x] {
				return
				//日志已经发完，下一个是心跳包
			}			
		} else {
			DPrintf("Rollback! leader %v to %v\n", rf.me, x)
			rf.nextIndex[x]--
		}		
	}
}

func (rf *Raft) HeartBeat() {
	for {
		if rf.killed() {
			return
		}
		_, isLeader := rf.GetState()
		if !isLeader { 
			return
		}		

		for i:=0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			go rf.AppendEntries(i)
		}	

		time.Sleep(time.Duration(DefaultHeartBeatTimeout) * time.Millisecond)	
	}
}


/*
 * 作用：领导选举
 * candidate并行发送requestvote RPC，收集选票
 * 如果candidate票数足够，变成leader并发送心跳
 * 否则等待选举超时，开始新一轮的选举
 * 一段时间没有响应就超时重发，收到响应后就不重发了
 */

func (rf *Raft) LeaderElection() {
	isFinish := false
	for {
		if rf.killed() {
			return
		}
		rf.mu.Lock()
		currentTerm := rf.currentTerm
		state := rf.state
		lastLogIndex := len(rf.logs) - 1
		lastLogTerm := rf.logs[lastLogIndex].Term
		rf.mu.Unlock()	

		if state != Candidate { 
			//发送中，收到Leader心跳或更大的Term，变回follower。因为是并行发送,可能性很小
			return
		}

		for i:=0; i < len(rf.peers); i++ {
			if i == rf.me || rf.voteGranted[i] != -1 {
				continue
			}
			go func (x, currentTerm int) {
				args := RequestVoteArgs {
					Term : currentTerm,
					CandidateId : rf.me,
					LastLogIndex : lastLogIndex,
					LastLogTerm : lastLogTerm,
				}
				reply := RequestVoteReply{}

				DPrintf("Requestvote begin: %v to %v!\n", rf.me, x)
				startTime := time.Now()
				ok := rf.sendRequestVote(x, &args, &reply)
				durTime := time.Now().Sub(startTime)
				DPrintf("Requestvote end: %v to %v and time is %v!\n", rf.me, x, durTime)

				if ok == false {
					DPrintf("Requestvote return false: %v to %v!\n", rf.me, x)
					return
				}

				rf.mu.Lock()
				defer rf.mu.Unlock()

				if rf.state != Candidate && currentTerm == rf.currentTerm {
					//确保是当前任期的Candidate
					return
				}

				if rf.currentTerm < reply.Term {
					rf.state = Follower
					rf.currentTerm = reply.Term
					return
				}

				if reply.VoteGranted {
					DPrintf("%v wins the vote from %v!\n", rf.me, x)
					rf.voteGranted[x] = 1
				} else {
					DPrintf("%v loses the vote from %v!\n", rf.me, x)
					rf.voteGranted[x] = 0
				}

				voteCount := 0
				isDone := true
				for i:=0; i < len(rf.peers); i++ {
					if rf.voteGranted[i] == 1 {
						voteCount++
					}
					if rf.voteGranted[i] == -1 {
						isDone = false
					}
				}
				
				isFinish = isDone
			
				if voteCount * 2 > len(rf.peers) {
					isFinish = true
					rf.state = Leader
					rf.nextIndex = make([]int, len(rf.peers))
					rf.matchIndex = make([]int, len(rf.peers))
					for i := 0; i < len(rf.peers); i++ {
						rf.nextIndex[i] = len(rf.logs)
					}
					DPrintf("%v becomes leader in term %v!\n\n", rf.me, rf.currentTerm)
					go rf.HeartBeat()					
				}
			}(i, currentTerm)			
		}

		if isFinish {
			break
		}
		time.Sleep(time.Duration(DefaultHeartBeatTimeout) * time.Millisecond)
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
			DPrintf("%v will sleep %v!\n", rf.me, time.Duration(electionTimeout) * time.Millisecond - startTime.Sub(lastTime))
			time.Sleep(time.Duration(electionTimeout) * time.Millisecond - startTime.Sub(lastTime))
		} else {
			DPrintf("%v no sleep!\n", rf.me)
		}
		
		if rf.killed() {
			return
		}
		rf.mu.Lock()
		if rf.state != Leader && rf.lastReceive.Equal(lastTime) {
			rf.state = Candidate
			rf.currentTerm++
			rf.voteFor = rf.me
			for i := 0; i < len(rf.peers); i++ {
				rf.voteGranted[i] = -1
			}
			rf.voteGranted[rf.me] = 1
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
	rf.state = Follower
	rf.currentTerm = 0
	rf.lastReceive = time.Now()
	rf.voteGranted = make([]int, len(peers))
	for i := 0; i < len(peers); i++ {
		rf.voteGranted[i] = -1
	}

	//2B	
	rf.applyCh = applyCh
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	for i := 0; i < len(peers); i++ {
		rf.nextIndex[i] = 1
	}
	rf.logs = []LogEntry{}
	nullLog := LogEntry{}
	rf.logs = append(rf.logs, nullLog)

	rf.logstates = []LogState{}
	nullLogState := LogState{}
	rf.logstates = append(rf.logstates, nullLogState)
	go rf.CheckElectionTimeout()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
