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
	"math/rand"
	"sync"
	"time"
)
import "labrpc"

// import "bytes"
// import "labgob"
const (
	Leader = iota
	Candidate
	Follower
)
// raft 状态

const (
	MINELECTIONTIMEOUT  = 200
	MAXELECTIONTIMEOUT  = 300
	HeartbeatCycle  = time.Millisecond * 50
)
// 选举 过期时间随机范围


// 判断是不是已经达到一半
func (rf *Raft) major() bool{
	return rf.numVote >= (len(rf.peers) / 2 + 1)
}


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

// log item in Log
type LogItem struct {
	Command interface{}
	Term int
}



//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	log       []LogItem           // log stored in each node.

	currentTerm int               // latest term server has seen

	votedFor int                  // candidateId that received vote in current term

	numVote int                   // numVote got

	state    int                  // if the raft candidate is Leader or candidate or follower.

	commitIndex int               // index of highest log entry known to be committed.

	lastApplied int               // index of highest log entry applied to state machine.

	nextIndex   []int             // for each server, index of the next log entry to send to that server

	matchIndex  []int             // for each server, index of highest log entry known to be replicated on server

	applyChan   chan ApplyMsg     // Apply channel

	timer       *time.Timer       // timer for the raft node.


	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
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




//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term int // candidate's term

	CandidateId int  // candidateId

	LastLogIndex int // lastLogIndex

	LastLogTerm int // last log term.

}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term int // currentTerm, for candidate to update itself

	VoteGranted bool // true means candidate received vote

}

// compare two log entries and return if the args log entry is more up to date
func (rf *Raft) isCandidateUpToDate(args *RequestVoteArgs) bool {
	/*
	Raft determines which of two logs is more up-to-date by
	comparing the index and term of the last entries in the logs.
	If the logs have last entries with different terms, then the log with the
	later term is more up-to-date. If the logs end with the same term,
	then whichever log is longer is more up-to-date.
	 */
	if args.Term < rf.currentTerm {
		return false
	}
	if args.LastLogTerm < rf.currentTerm {
		return false
	}
	if args.LastLogIndex < len(rf.log) - 1 {
		return false
	}
	return true
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	/*
	If votedFor is null or candidateId, and candidate’s
	log is at least as up-to-date as receiver’s log, grant vote
	 */
	if rf.isCandidateUpToDate(args) &&
		(rf.votedFor == -1 || rf.votedFor == args.CandidateId) {
		// grant vote and update rf's term.
		rf.currentTerm = args.Term

		reply.Term = args.Term

		reply.VoteGranted = true
	} else {
		// don't grant vote to the candidate.
		reply.Term = rf.currentTerm

		reply.VoteGranted = false
	}

}

type AppendEntryArgs struct {
	term int // leader’s term

	leaderId int // so follower can redirect clients

	prevLogIndex int // index of log entry immediately preceding new ones

	prevLogTerm int // term of prevLogIndex entry

	entries []LogItem // log entries to store (empty for heartbeat; may send more than one for efficiency)

	leaderCommit int // leader’s commitIndex

}

type AppendEntryReply struct {
	term int // for leader to update itself

	success bool // true if follower
	// contained entry matching prevLogIndex and prevLogTerm
}


func (rf *Raft) AppendEntries(args *AppendEntryArgs, reply *AppendEntryReply) {
	/*1. Reply false if term < currentTerm (§5.1)
	2. Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
	3. If an existing entry conflicts with a new one (same index but different terms),
	delete the existing entry and all that follow it (§5.3)
	4. Append any new entries not already in the log
	5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	 */
	// 1. old leader.

	if rf.currentTerm > args.term {
	 	reply.term = rf.currentTerm
	 	reply.success = false
	 	return
	 	}
	rf.state = Follower
	// 2. too short rf log entry
	if args.prevLogIndex > len(rf.log) - 1 { // log is too short
		reply.term = rf.currentTerm

		reply.success = false
		// 我们什么也不需要做，因为rpc失败后args.prevLogIndex会递减
		return
	}

	// 3. not match(conflict entrys)
	if args.prevLogTerm != rf.log[args.prevLogTerm].Term {
		// 首先删除掉对应的不匹配的entry及之后的entry
		rf.log = rf.log[:args.prevLogIndex]

		reply.term = rf.currentTerm

		reply.success = false

		return
	}

	// 匹配成功的情况
	reply.success = true
	reply.term = args.term
	// 1.添加entry
	for _, entry := range args.entries {
		rf.log = append(rf.log, entry)
	}
	// 2. 更新commitIndex(已知的最大commitIndex)
	if args.leaderCommit > rf.commitIndex {
		if args.leaderCommit > len(rf.log) - 1 {
			rf.commitIndex = len(rf.log) - 1
		}else {
			rf.commitIndex = args.leaderCommit
		}
	}
	// 3. 更新rf.currentTerm
	rf.currentTerm = args.term

	// 4. applied 已经确认提交的entry
	for i := rf.lastApplied + 1 ; i <= rf.commitIndex ; i++ {
		applymsg := ApplyMsg{true, rf.log[i].Command, i}
		rf.applyChan <- applymsg
	}

	rf.lastApplied = rf.commitIndex
	return
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

// https://github.com/golang/go/wiki/CommonMistakes
func (rf *Raft) sendRequestVoteToAll(args *RequestVoteArgs, reply *RequestVoteReply) {
	for server := 0; server < len(rf.peers); server++ {
		go func(server int) {
			ok := rf.sendRequestVote(server, args, reply)
			if ok {
				rf.numVote++
			}
		}(server)
	}

}


func (rf *Raft) sendAppendEntries(server int, args *AppendEntryArgs, reply *AppendEntryReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	//If successful: update nextIndex and matchIndex for follower (§5.3)
	// If AppendEntries fails because of log inconsistency:
	// decrement nextIndex and retry (§5.3)
	/*
	If last log index ≥ nextIndex for a follower:
	send AppendEntries RPC with log entries starting at nextIndex
	 */
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

	// Your code here (2B).


	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}


// deal with the situation of time-out
func (rf *Raft) handleTimer() {
	// 1. timeout的是follower. 变为follower
	/*
	If election timeout elapses without receiving AppendEntries
	RPC from current leader or granting vote to candidate:
	convert to candidate
	 */
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state == Follower {
		rf.resetTimer()
		rf.currentTerm += 1
		rf.votedFor = rf.me
		rf.state = Candidate
		args := &RequestVoteArgs{
			rf.currentTerm,
			rf.me,
			len(rf.peers) - 1,
			rf.log[len(rf.peers) - 1].Term,
		}
		reply := &RequestVoteReply{}
		rf.sendRequestVoteToAll(args, reply)
		if rf.major() {
			// success. become leader.

		}

	}else if rf.state == Leader {

	}



}


// reset timer.
func (rf *Raft) resetTimer() {
	if rf.timer == nil {
		rf.timer = time.NewTimer(time.Millisecond * 1000)
		go func() {
			for {
				<-rf.timer.C
				rf.handleTimer()
			}
		}()
	}
	new_timeout := HeartbeatCycle
	if rf.state != Leader {
		new_timeout = time.Millisecond * time.Duration(MINELECTIONTIMEOUT+rand.Int63n(MAXELECTIONTIMEOUT-MINELECTIONTIMEOUT))
	}
	rf.timer.Reset(new_timeout)
	// rf.logger.Printf("Resetting timeout to %v\n", new_timeout)
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
	rf.numVote = 0
	rf.log = make([]LogItem, 0)
	rf.votedFor = -1
	rf.currentTerm = 0
	rf.state = Follower
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.commitIndex = -1
	rf.lastApplied = -1

	// random generate a duration
	//randomTimeOut := rand.Intn(MAXELECTIONTIMEOUT - MINELECTIONTIMEOUT) + MINELECTIONTIMEOUT

	//rf.timer = time.NewTimer(time.Duration(randomTimeOut))

	// Your initialization code here (2A, 2B, 2C).
	// 触发一定时间内没有收到heartbeat进行选举


	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.resetTimer()



	return rf
}
