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
	//	"bytes"
	// "log"
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}


type Log struct{
	Terms 	int32
	Entry 	*ApplyMsg
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm 	int32
	votedFor 		int32
	logs 			[]*Log

	commitIndex 	int32
	lastApplied 	int32

	nextIndexList 	[]int32
	matchIndexList 	[]int32

	role 		int32
	leaderId 	int32
	heart 		bool
}

const(
	Role_Follower = 1
	Role_Candidate = 2
	Role_Leader = 3

	election_timeout = 1000
)

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	term = int(rf.currentTerm)
	isleader = rf.role == Role_Leader
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}


// restore previously persisted state.
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


// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}


// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term 			int32
	CandidateId 	int32
	LastLogIndex	int32
	LastLogTerm		int32
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term 		int32
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	// 校验
	if rf.currentTerm > args.Term{
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 更新
	if rf.currentTerm < args.Term{
		rf.currentTerm = args.Term
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
		// log.Printf("server %v : to follower", rf.me)
		rf.role = Role_Follower
		rf.votedFor = args.CandidateId
		return
	}
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && int32(len(rf.logs) - 1) <= args.LastLogIndex && rf.logs[len(rf.logs) - 1].Terms <= args.LastLogTerm{
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
	}
}

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	// log.Printf("RequestVote: %v -> %v,  args:%v,  replay:%v,   ok:%v", rf.me, server, args, reply, ok)
	return ok
}



type AppendEntriesArgs struct {
	Term 			int32
	LeaderId 		int32
	PrevLogIndex	int32
	PrevLogTerm		int32
	Entries			[]*ApplyMsg
	LeaderCommit	int32
}

type AppendEntriesReply struct {
	Term			int32
	Success			bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// 校验
	if rf.currentTerm > args.Term{
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 更新
	if rf.currentTerm < args.Term{
		rf.currentTerm = args.Term
		reply.Term = rf.currentTerm
		reply.Success = false
		// log.Printf("server %v : role from %v to follower, leaderid from %v to %v", rf.me, rf.role, rf.leaderId, args.LeaderId)
		rf.role = Role_Follower
		rf.leaderId = args.LeaderId
		rf.heart = true
		return
	}
	reply.Term = rf.currentTerm
	reply.Success = false
	// if rf.leaderId >= 0 && rf.leaderId != args.LeaderId{
	// 	return
	// }
	rf.role = Role_Follower
	rf.leaderId = args.LeaderId
	rf.heart = true
	// // 校验
	// if rf.logs[args.PrevLogIndex].Terms != args.Term{
	// 	// 删除错误的提交
	// 	rf.logs = rf.logs[:args.PrevLogIndex]
	// 	return
	// }
	// reply.Success = true
	// //追加entry
	// for _, entry := range args.Entries{
	// 	rf.logs = append(rf.logs, &Log{
	// 		Terms: args.Term,
	// 		Entry: entry,
	// 	})
	// }
	// if rf.commitIndex < args.LeaderCommit{
	// 	rf.commitIndex = int32(math.Min(float64(args.LeaderCommit), float64(len(rf.logs) - 1)))
	// }
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	// log.Printf("AppendEntries: %v -> %v,  args:%v,  replay:%v,  ok:%v", rf.me, server, args, reply, ok)
	return ok
}


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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := false

	// Your code here (2B).
	if rf.role != Role_Leader{
		return index, term, isLeader
	}
	isLeader = true
	term = int(rf.currentTerm)
	index = len(rf.logs)
	rf.logs = append(rf.logs, &Log{
		Terms: rf.currentTerm,
		Entry: &ApplyMsg{
			CommandValid: true,
			Command: command,
			CommandIndex: index,
		},
	})
	for idx, _ := range rf.peers{
		if idx == rf.me{
			continue
		}
		go BoardCastAppendEntries(rf, idx)
	}
	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	// log.Printf("server %v : begin...", rf.me)
	for rf.killed() == false {

		// Your code here (2A)
		// Check if a leader election should be started.

		// 超时
		ms := 1000 + (rand.Int63() % 1000)
		time.Sleep(time.Duration(ms) * time.Millisecond)
		if rf.role != Role_Leader{
			rf.mu.Lock()
			if !rf.heart{
				// log.Printf("server %v : receive no heart", rf.me)
				rf.leaderId = -1
				rf.votedFor = -1
			}
			rf.heart = false
			rf.mu.Unlock()
		}
		if rf.role == Role_Follower{
			if rf.leaderId == -1 && rf.votedFor == -1{
				// log.Printf("server %v : follower to candidate", rf.me)
				rf.mu.Lock()
				rf.role = Role_Candidate
				rf.mu.Unlock()
			}
		}
		if rf.role == Role_Candidate{
			// 准备工作
			rf.mu.Lock()
			rf.currentTerm += 1
			rf.votedFor = int32(rf.me)
			rf.heart = true
			rf.mu.Unlock()
			// 请求投票
			resultChan := make(chan bool)
			for idx, _ := range rf.peers{
				if idx == rf.me{
					continue
				}
				go BoardCastRequestVote(rf, idx, resultChan)
			}
			magorNum := int32(math.Ceil(float64(len(rf.peers)) / 2.0))
			voteGrantedNum := int32(1)
			voteRejectNum := int32(0)
			// // log.Printf("server %v : magorNum : %v", rf.me, magorNum)
			for {
				result := <- resultChan
				// // log.Printf("server %v : result : %v", rf.me, result)
				if rf.role != Role_Candidate{
					break
				}
				if result{
					voteGrantedNum += 1
					if  voteGrantedNum >= magorNum{
						// log.Printf("server %v : candidate to leader", rf.me)
						close(resultChan)
						rf.mu.Lock()
						rf.role = Role_Leader
						rf.leaderId = int32(rf.me)
						rf.mu.Unlock()
						break
					}
				}else{
					voteRejectNum += 1
					if  voteRejectNum >= magorNum{
						// log.Printf("server %v : remain candidate", rf.me)
						close(resultChan)
						break
					}
				}
			}
		}
		if rf.role == Role_Leader{
			// 心跳 
			for idx, _ := range rf.peers{
				if idx == rf.me{
					continue
				}
				go BoardCastAppendEntries(rf, idx)
			}
			// todo 处理响应结果
		}
		
		// pause for a random amount of time between 50 and 350
		// milliseconds.
		// ms := 50 + (rand.Int63() % 300)
		// time.Sleep(time.Duration(ms) * time.Millisecond)
	}
	// log.Printf("server %v : exit", rf.me)
}

func BoardCastRequestVote(rf *Raft, idx int, result chan bool)  {
	args := RequestVoteArgs{
		Term: rf.currentTerm,
		CandidateId: int32(rf.me),
		LastLogIndex: int32(len(rf.logs)-1),
		LastLogTerm: rf.logs[len(rf.logs)-1].Terms,
	}
	reply := RequestVoteReply{}
	ok := rf.sendRequestVote(idx, &args, &reply)	

	if !ok{
		result <- false
		return
	}
	if rf.currentTerm < reply.Term{
		// log.Printf("server %v : candidate to follower", rf.me)
		rf.mu.Lock()
		rf.currentTerm = reply.Term
		rf.role = Role_Follower
		rf.votedFor = -1
		rf.mu.Unlock()
		result <- false
		return
	}
	if reply.VoteGranted{
		result <- true
		return
	}
	result <- false
}

func BoardCastAppendEntries (rf *Raft, idx int)  {
	args := AppendEntriesArgs{
		Term: rf.currentTerm,
		LeaderId: int32(rf.me),
		PrevLogIndex: int32(len(rf.logs) - 1),
		PrevLogTerm: rf.logs[int32(len(rf.logs) - 1)].Terms,
		Entries: make([]*ApplyMsg, 0),
		LeaderCommit: rf.commitIndex,
	}
	reply := AppendEntriesReply{}
	ok := rf.sendAppendEntries(idx, &args, &reply)	
	if !ok{
		return
	}
	
	if rf.currentTerm < reply.Term{
		// log.Printf("server %v : leader to follower", rf.me)
		rf.mu.Lock()
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		rf.role = Role_Follower
		rf.mu.Unlock()
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	// log.Printf("server %v : init...", rf.me)
	rf.currentTerm = 0
	rf.votedFor = -1  // 还没有投票
	rf.logs = make([]*Log, 0)
	rf.logs = append(rf.logs, &Log{
		Terms: -1,
		Entry: nil,
	})

	rf.commitIndex = 0
	rf.lastApplied = 0
	
	rf.role = Role_Follower
	rf.leaderId = -1 // 还没有leader
	rf.heart = false
	go func ()  {
		for rf.killed() == false {
			ms := 150 + (rand.Int63() % 150)
			time.Sleep(time.Duration(ms) * time.Millisecond)
			if rf.role == Role_Leader{
				// // log.Printf("server %v : send heart", rf.me)
				for idx, _ := range rf.peers{
					if idx == rf.me{
						continue
					}
					go BoardCastAppendEntries(rf, idx)
				}
			}
		}
	}()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()


	return rf
}
