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
import "log"

import "bytes"
import "../labgob"

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

const (
	Follower string = "follower"
	Candidate string = "canditate"
	Leader string = "leader"
	DefaultElectionTimeout int = 400
	DefaultHeartBeatTimeout int = 150
	//优化
	MultLogs bool = true	// 一次携带多个log
	FastBackup bool = true	// 快速定位
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
	logstates	[]int
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

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.voteFor)
	e.Encode(len(rf.logs) - 1)
	for i:=1;i<len(rf.logs);i++ {
		e.Encode(rf.logs[i])
	}
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
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

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm, voteFor, logLen int
	if d.Decode(&currentTerm) != nil ||
	   d.Decode(&voteFor) != nil ||
	   d.Decode(&logLen) != nil {
	  DPrintf("Decode fail!\n")
	} else {
	  rf.currentTerm = currentTerm
	  rf.voteFor = voteFor
	}
	rf.logs = []LogEntry{}
	rf.logs = append(rf.logs, LogEntry{})
	for i:=0;i<logLen;i++ {
		newLog := LogEntry{}
		if d.Decode(&newLog) != nil {
			DPrintf("Decode fail!\n")
		} else {
			rf.logs = append(rf.logs, newLog)
		}
	}
	rf.logstates = make([]int, len(rf.logs))
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
	XTerm	int
	XIndex 	int
	XLen	int
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
 * 只有成功投票和失败投票时Leader变回Follower会重置定时器
 * 防止拥有过期日志的candidate反复超时，而拥有新日志的follower不超时
 * Term变大意味着开始下一个任期，必重置定时器
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
		rf.persist()
	} else {
		reply.VoteGranted = false
		if args.Term > rf.currentTerm {
			rf.currentTerm = args.Term
			rf.voteFor = -1
			if rf.state == Leader {
				rf.lastReceive = time.Now()
			}
			rf.state = Follower
			rf.persist()
		}
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
				if MultLogs {
					oldLogs := rf.logs[0 : args.PrevLogIndex]
					oldLogs = append(oldLogs, rf.logs[args.PrevLogIndex])

					rf.logs = append(oldLogs, args.Logs...)
					DPrintf("%v add log[%v]-log[%v]\n", rf.me, args.PrevLogIndex + 1, args.PrevLogIndex + len(args.Logs))
					rf.logstates = make([]int, len(rf.logs))
				} else {
					newLog := args.Logs[0]
					if len(rf.logs) > args.PrevLogIndex + 1 {
						rf.logs[args.PrevLogIndex + 1] = newLog 
						DPrintf("%v rewrite log[%v]\n", rf.me, args.PrevLogIndex + 1)
					} else {
						rf.logs = append(rf.logs, newLog)
						DPrintf("%v add log[%v]\n", rf.me, args.PrevLogIndex + 1)
					}
					rf.logstates = make([]int, len(rf.logs))
				}
			}
			if args.LeaderCommit > rf.commitIndex {
				rf.commitIndex = args.LeaderCommit
				if rf.commitIndex > len(rf.logs) - 1 {
					rf.commitIndex = len(rf.logs) - 1
				}
				lastApplied := rf.lastApplied
				for rf.lastApplied < rf.commitIndex {
					rf.lastApplied++
					rf.applyCh <- rf.logs[rf.lastApplied].Msg
				}
				if lastApplied != rf.lastApplied {
					DPrintf("%v applys log[%v]-log[%v]!\n", rf.me, lastApplied+1, rf.lastApplied)
				}
			}
		} else {
			reply.Success = false
			if args.PrevLogIndex == 0 {
				log.Fatalf("AppendEntriesRPC error: reply failed and prelogindex == 0")
			}
			if FastBackup {
				if len(rf.logs) - 1 >= args.PrevLogIndex {
					reply.XTerm = rf.logs[args.PrevLogIndex].Term
					reply.XIndex = rf.logs[args.PrevLogIndex].Index
					for i := args.PrevLogIndex; i >= 0; i--{
						if rf.logs[i].Term != reply.XTerm {
							break
						}
						reply.XIndex = rf.logs[i].Index
					}
					// reply.XLen = args.PrevLogIndex 
				} else {
					reply.XTerm = -1
					// reply.XIndex = args.PrevLogIndex 
					reply.XLen = args.PrevLogIndex - len(rf.logs) + 1
				}
			}
			
		}	
		rf.persist()
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

func (rf *Raft) ShowState() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("\nRAFT STATE %v:\n", rf.me)
	DPrintf("term %v state %v loglen %v commitIndex %v lastApplied %v\n", 
	rf.currentTerm, rf.state, len(rf.logs) - 1, rf.commitIndex, rf.lastApplied)
	if rf.state == Leader {
		for i:=0;i<len(rf.peers);i++ {
			if i==rf.me {
				continue
			}
			DPrintf("%v matchIndex %v nextIndex %v\n", i, rf.matchIndex[i], rf.nextIndex[i])
		}
	}
	DPrintf("\n")
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
		newLogState := 0
		rf.logs = append(rf.logs, newLog)
		rf.logstates = append(rf.logstates, newLogState)
		rf.persist()
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

func (rf *Raft) AppendEntries(x, beginTerm int) {
	for {
		if rf.killed() {
			DPrintf("%v has been killed\n", rf.me)
			return
		}

		rf.mu.Lock()
		isLeader := rf.state == Leader
		if !isLeader || rf.currentTerm != beginTerm {
			rf.mu.Unlock()	
			return
		}	
		
		nextIndex := rf.nextIndex[x]
		prevLogIndex := nextIndex - 1
		prevLogTerm := rf.logs[prevLogIndex].Term
		leaderCommit := rf.commitIndex
		matchIndex := rf.matchIndex[x]

		// 在每次只发一个log的情况下，leaderCommit需要限制，否则可能导致错误提交。
		// 比如follower后面的log都是错误的，而当前只重写了一个log。
		// 很大的leaderCommit会导致follower错误提交后续的log。
		// 所以leaderCommit最大只能是当前nextIndex值，用于让follower复制完成后立马提交该log。
		if !MultLogs {
			if leaderCommit > nextIndex {
				leaderCommit = nextIndex
			}
		}

		args := AppendEntriesArgs {
			Term : rf.currentTerm,
			PrevLogIndex : prevLogIndex,
			PrevLogTerm : prevLogTerm, 
			LeaderCommit : leaderCommit,
		}

		args.Logs = []LogEntry{}

		if len(rf.logs) == nextIndex {
			args.IsHeartBeat = true
		} else {
			args.IsHeartBeat = false
			if MultLogs {
				args.Logs = rf.logs[nextIndex : ]
			} else {
				args.Logs = append(args.Logs, rf.logs[nextIndex])
			}
		}

		reply := AppendEntriesReply{}
		sendLogLen := len(rf.logs) - nextIndex

		rf.mu.Unlock()	

		if args.IsHeartBeat {
			DPrintf("HB begin: %v in term %v send (%v %v %v) to %v!\n", rf.me, beginTerm, prevLogIndex, prevLogTerm, leaderCommit, x)
		} else {
			if !MultLogs {
				DPrintf("AE begin: %v in term %v send log[%v] to %v!\n", rf.me, beginTerm, prevLogIndex + 1, x)
			} else {
				DPrintf("AE begin: %v in term %v send log[%v]-log[%v] to %v!\n", rf.me, beginTerm, prevLogIndex + 1, nextIndex + sendLogLen - 1, x)
			}
			
		}
		
		ok := rf.sendAppendEntries(x, &args, &reply)

		if args.IsHeartBeat {
			DPrintf("HB end: %v in term %v send (%v %v %v) to %v!\n", rf.me, beginTerm, prevLogIndex, prevLogTerm, leaderCommit, x)
		} else {
			if !MultLogs {
				DPrintf("AE end: %v in term %v send log[%v] to %v!\n", rf.me, beginTerm, prevLogIndex + 1, x)
			} else {
				DPrintf("AE end: %v in term %v send log[%v]-log[%v] to %v!\n", rf.me, beginTerm, prevLogIndex + 1, nextIndex + sendLogLen - 1, x)
			}
		}

		if rf.killed() {
			return
		}
		if !ok {
			// DPrintf("AE return false!\n")
			return
		}

		rf.mu.Lock()

		isLeader = rf.state == Leader
		if !isLeader || args.Term != rf.currentTerm || rf.matchIndex[x] != matchIndex || nextIndex != rf.nextIndex[x] {
			// DPrintf("%v is not leader or term changed\n", rf.me)
			rf.mu.Unlock()	
			return
		}		

		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.state = Follower
			rf.voteFor = -1
			rf.lastReceive = time.Now() 
			DPrintf("%v become follower\n", rf.me)
			rf.persist()
			rf.mu.Unlock()	
			return
		} 
		
		if reply.Success {
			DPrintf("Success! %v to %v\n", rf.me, x)
			if !args.IsHeartBeat {
				if MultLogs {
					successLogIndex := rf.matchIndex[x] + 1
					rf.matchIndex[x] = nextIndex + sendLogLen - 1
					rf.nextIndex[x] = rf.matchIndex[x] + 1
					
					for successLogIndex <= rf.matchIndex[x] {
						if rf.logs[successLogIndex].Term == rf.currentTerm {
							rf.logstates[successLogIndex]++
							if (rf.logstates[successLogIndex] + 1) * 2 > len(rf.peers) {
								rf.commitIndex = rf.logs[successLogIndex].Index
								for rf.lastApplied < rf.commitIndex {
									rf.lastApplied++
									rf.applyCh <- rf.logs[rf.lastApplied].Msg
									DPrintf("Leader %v applys log %v !\n", rf.me, rf.lastApplied)
								}
							}
						}
						successLogIndex++
					}
				} else {
					rf.matchIndex[x] = rf.nextIndex[x]
					rf.nextIndex[x]++
					successLogIndex := rf.matchIndex[x]
					if rf.logs[successLogIndex].Term == rf.currentTerm {
						//当前Term的log可以直接根据计数提交，无视乱序接受RPC响应
						rf.logstates[successLogIndex]++
						if (rf.logstates[successLogIndex] + 1) * 2 > len(rf.peers) {
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

			}
			if len(rf.logs) == rf.nextIndex[x] {
				rf.mu.Unlock()	
				return
				//日志已经发完，下一个是心跳包
			}			
		} else {
			DPrintf("FAIL! Rollback! %v to %v\n", rf.me, x)
			err_msg := ""
			if FastBackup {
				if reply.XTerm == -1 {
					rf.nextIndex[x] -= reply.XLen
					DPrintf("Rollback: rf.nextIndex[%v] from %v to %v\n", x, nextIndex, rf.nextIndex[x])
				} else if reply.XTerm > 0 {
					matchLogIndex := reply.XIndex
					for i := 1; i <= len(rf.logs) - 1; i++ {
						if rf.logs[i].Term == reply.XTerm {
							matchLogIndex = rf.logs[i].Index
							break
						}
					}
					rf.nextIndex[x] = matchLogIndex
					DPrintf("Rollback: rf.nextIndex[%v] from %v to %v\n", x, nextIndex, rf.nextIndex[x])
				} else {
					err_msg = "Reply.XTerm == 0"
				}
				if rf.nextIndex[x] < 1 {
					err_msg = "rf.nextIndex[x] < 1"
				}			
			} else {
				rf.nextIndex[x]--
			}
			if err_msg != "" {
				log.Fatalf("Rollback error: %v (%v %v %v) %v\n", err_msg, reply.XTerm, reply.XIndex, reply.XLen, rf.nextIndex[x])
			}
		}		
		rf.mu.Unlock()	
	}
}

func (rf *Raft) HeartBeat(beginTerm int) {
	for {
		if rf.killed() {
			return
		}
		currentTerm, isLeader := rf.GetState()
		if !isLeader || currentTerm != beginTerm { 
			return
		}		

		for i:=0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			go rf.AppendEntries(i, currentTerm)
		}	

		time.Sleep(time.Duration(DefaultHeartBeatTimeout) * time.Millisecond)	
	}
}

func (rf *Raft) RequestVote(x int, args RequestVoteArgs, reply RequestVoteReply) {
	if rf.killed() {
		return
	}
	rf.mu.Lock()
	if rf.RequestVoteFinish(args.Term) {
		rf.mu.Unlock()	
		return	//因为是并行发送,可能性很小
	}	
	rf.mu.Unlock()		

	DPrintf("Requestvote begin: %v to %v!\n", rf.me, x)
	// startTime := time.Now()
	ok := rf.sendRequestVote(x, &args, &reply)
	// durTime := time.Now().Sub(startTime)
	DPrintf("Requestvote end: %v to %v!\n", rf.me, x)	

	if ok == false {
		// DPrintf("Requestvote return false: %v to %v!\n", rf.me, x)
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()	

	if rf.RequestVoteFinish(args.Term) {
		return
	}

	if rf.currentTerm < reply.Term {
		rf.state = Follower
		rf.currentTerm = reply.Term
		rf.persist()
		return
	}

	if reply.VoteGranted {
		DPrintf("%v wins the vote from %v!\n", rf.me, x)
		rf.voteGranted[x] = 1

		voteCount := 0
		for i:=0; i < len(rf.peers); i++ {
			if rf.voteGranted[i] == 1 {
				voteCount++
			}
		}
		if voteCount * 2 > len(rf.peers) {
			rf.state = Leader
			rf.nextIndex = make([]int, len(rf.peers))
			rf.matchIndex = make([]int, len(rf.peers))
			for i := 0; i < len(rf.peers); i++ {
				rf.nextIndex[i] = len(rf.logs)
			}
			DPrintf("%v becomes leader in term %v!\n\n", rf.me, rf.currentTerm)
			go rf.HeartBeat(rf.currentTerm)					
		}
	} else {
		DPrintf("%v loses the vote from %v!\n", rf.me, x)
		rf.voteGranted[x] = 0
	}			
}

func (rf *Raft) RequestVoteFinish(Term int) bool {
	if rf.state != Candidate || Term != rf.currentTerm {
		//变回follower或leader。
		//是candidate，但是任期改变		
		return true
	} else {
		successVoteCount := 0
		failVoteCount := 0
		for i:=0; i < len(rf.peers); i++ {
			if rf.voteGranted[i] == 1 {
				successVoteCount++
			} else if rf.voteGranted[i] == 0 {
				failVoteCount++
			}		
		}
		if successVoteCount * 2 > len(rf.peers) || failVoteCount * 2 > len(rf.peers) {
			return true
		} 
	}
	return false
}

/*
 * 作用：领导选举
 * candidate并行发送requestvote RPC，收集选票
 * 如果candidate票数足够，变成leader并发送心跳
 * 否则等待选举超时，开始新一轮的选举
 * 一段时间没有响应就超时重发，收到响应后就不重发了
 */

func (rf *Raft) LeaderElection(beginTerm int) {
	for {
		if rf.killed() {
			return
		}

		rf.mu.Lock()
		if rf.RequestVoteFinish(beginTerm) {
			rf.mu.Unlock()	
			return
		}
		
		lastLogIndex := len(rf.logs) - 1
		lastLogTerm := rf.logs[lastLogIndex].Term

		args := RequestVoteArgs {
			Term : rf.currentTerm,
			CandidateId : rf.me,
			LastLogIndex : lastLogIndex,
			LastLogTerm : lastLogTerm,
		}
		reply := RequestVoteReply{}		

		RequestVoteIndex := []int{}

		for i:=0; i < len(rf.peers); i++ {
			if rf.voteGranted[i] != -1 {
				continue
			}			
			RequestVoteIndex = append(RequestVoteIndex, i)
		}
		
		if len(RequestVoteIndex) == 0 {
			log.Fatalf("ERROR! LeaderElection!\n")
		}
		rf.mu.Unlock()	

		for i:=0; i < len(RequestVoteIndex); i++ {
			go rf.RequestVote(RequestVoteIndex[i], args, reply)
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
			// DPrintf("%v will sleep %v!\n", rf.me, time.Duration(electionTimeout) * time.Millisecond - startTime.Sub(lastTime))
			time.Sleep(time.Duration(electionTimeout) * time.Millisecond - startTime.Sub(lastTime))
		} else {
			// DPrintf("%v no sleep!\n", rf.me)
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
			rf.persist()
			DPrintf("%v election timeout!\n", rf.me)
			go rf.LeaderElection(rf.currentTerm)
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
	rf.currentTerm = 1
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
	rf.logs = []LogEntry{}
	nullLog := LogEntry{}
	rf.logs = append(rf.logs, nullLog)

	rf.logstates = []int{}
	nullLogState := 0
	rf.logstates = append(rf.logstates, nullLogState)

	// 2C
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	DPrintf("Recover %v: term %v vote %v log[1]-log[%v] \n", rf.me, rf.currentTerm, rf.voteFor, len(rf.logs) - 1)
	for i := 0; i < len(peers); i++ {
		rf.nextIndex[i] = len(rf.logs)
	}
	go rf.CheckElectionTimeout()

	return rf
}
