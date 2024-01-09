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

type LogEntry struct {
	Command interface{}
	Term    int
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
	heartbeat int64 // the last time at which the peer heard from the leader
	isLeader  bool
	votes     int

	currentTerm int        // the latest term the peer has seen
	votedFor    int        // the peer that the peer voted for in the current term
	log         []LogEntry // the log of commands and term

	commitIndex int // the index of the highest log entry known to be committed
	lastApplied int // the index of the highest log entry applied to the state machine

	nextIndex  []int // for each peer, the index of the next log entry to send to that peer
	matchIndex []int // for each peer, the index of the highest log entry known to be replicated on that peer

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.isLeader
	rf.mu.Unlock()
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
	Term         int // the candidate's term
	CandidateId  int // the candidate requesting the vote
	LastLogIndex int // the index of the candidate's last log entry
	LastLogTerm  int // the term of the candidate's last log entry
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // the current term of the peer
	VoteGranted bool // true if the peer granted the vote
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	// fmt.Printf("peer=%d, currentTerm=%d, votedFor=%d\n", rf.me, rf.currentTerm, rf.votedFor)
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
	}
	if args.Term > rf.currentTerm {
		rf.votedFor = -1
	}
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		lastlogterm := -1
		if len(rf.log) != 0 {
			lastlogterm = -rf.log[len(rf.log)-1].Term
		}
		if (args.LastLogTerm >= lastlogterm) && (args.LastLogIndex >= len(rf.log)-1) {
			rf.votedFor = args.CandidateId
			rf.currentTerm = args.Term
			reply.VoteGranted = true
			rf.isLeader = false
		}
	}
	rf.mu.Unlock()
	// if reply.VoteGranted {
	// 	fmt.Printf("%d votes for %d\n", rf.me, args.CandidateId)
	// }
}

type AppendEntriesArgs struct {
	Term     int // the leader's term
	LeaderId int // the leader's id
}

type AppendEntriesReply struct{}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// fmt.Printf("Leader=%d follower=%d\n", args.LeaderId, rf.me)
	rf.mu.Lock()
	if rf.currentTerm < args.Term {
		rf.isLeader = false
	}
	rf.heartbeat = time.Now().UnixNano() / 1e6
	rf.mu.Unlock()
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
	if ok {
		rf.mu.Lock()
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.votes = 0
			rf.isLeader = false
			rf.votedFor = -1
		} else if reply.VoteGranted {
			rf.votes += 1
		}
		if !rf.isLeader && rf.votes > len(rf.peers)/2 {
			rf.isLeader = true
			for i := 0; i < len(rf.peers); i++ {
				if i != rf.me {
					args := AppendEntriesArgs{}
					args.Term = rf.currentTerm
					args.LeaderId = rf.me
					reply := AppendEntriesReply{}
					go rf.sendAppendEntries(i, &args, &reply)
				}
			}
		}
		rf.mu.Unlock()
	}
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := false
	rf.mu.Lock()
	isleader := rf.isLeader
	rf.mu.Unlock()
	for isleader {
		go rf.peers[server].Call("Raft.AppendEntries", args, reply)
		time.Sleep(time.Millisecond * 50)
	}
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
	isLeader := true

	// Your code here (2B).

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
	for !rf.killed() {
		// Your code here (2A)
		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
		// Check if a leader election should be started.
		rf.mu.Lock()
		isleader := rf.isLeader
		rf.mu.Unlock()
		if !isleader {
			currentTime := time.Now().UnixNano() / 1e6 // in milliseconds
			timeout := 150 + (rand.Int63() % 200)
			rf.mu.Lock()
			heartbeat := rf.heartbeat
			rf.mu.Unlock()
			if currentTime-heartbeat < timeout {
				continue
			}
			// fmt.Printf("[%d] %d start a election\n", time.Now().UnixNano()/1e6, rf.me)
			// otherwise, start a leader election
			rf.mu.Lock()
			rf.currentTerm++
			rf.votedFor = rf.me
			rf.votes = 1
			rf.heartbeat = time.Now().UnixNano() / 1e6
			numPeers := len(rf.peers)
			numLogs := len(rf.log)
			rf.mu.Unlock()
			for i := 0; i < numPeers; i++ {
				if i != rf.me {
					args := RequestVoteArgs{}
					reply := RequestVoteReply{}
					args.Term = rf.currentTerm
					args.CandidateId = rf.me
					if numLogs == 0 {
						args.LastLogTerm = -1
					} else {
						args.LastLogTerm = rf.log[numLogs-1].Term
					}
					args.LastLogIndex = numLogs - 1
					go rf.sendRequestVote(i, &args, &reply)
				}
			}
		}
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
	rf.votedFor = -1 // not vote for anyone in this term
	rf.currentTerm = 0
	rf.isLeader = false
	rf.heartbeat = time.Now().UnixNano() / 1e6
	rf.votes = 0
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
