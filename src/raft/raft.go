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
import "labrpc"
import "time"
import "math/rand"
import "fmt"
// import "bytes"
// import "encoding/gob"

// the 3 possible server status
const (
    FOLLOWER = iota
    CANDIDATE
    LEADER
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

type LogEntry struct {
    Log         interface{}
    Term        int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

    //persistent state on all servers
    currentTerm         int             //latest term server has seen
    votedFor            int             //candidateId that received vote in the current term
    log                 []LogEntry      //log entries. each entry has command and term when entry was received by leader

    //volatile on all servers
    commitIndex         int             //index of highest log entry known to be committed
    lastApplied         int             //index of highest log entry applied to state machine

    //volatile on leaders
    nextIndex           []int           //for each server, index of next log entry to send to it
    matchIndex          []int           //for each server, index of highest log entry known to be replicated on it

    //extra volatile stuff needed
    status              int             //server's current state, Follower, Candidate, Leader
	leaderId		    int				//leader's id stored in each server
    electionTimeout     time.Duration   //election timeout limit for current server
    electionTimer       time.Time       //election timer for current server
    heartbeatTimeout    time.Duration   //leader sends messages spaced by heartbeat timeout
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (2A).
    rf.mu.Lock()
    term = rf.currentTerm
    isleader = (rf.status == LEADER)
    rf.mu.Unlock()
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
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
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
    CandidatesTerm                  int         //Candidate's Term
    CandidateId                     int         //Candidate's Requesting Vote
    LastLogIndex                    int         //Index of candidate's last log entry
    LastLogTerm                     int         //Term of candidate's last log entry
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Success							bool		//Call Succeeded
    Term                            int         //current Term
    VoteGranted                     bool        //candidate received vote or not
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
    rf.mu.Lock()
    defer rf.mu.Unlock()
    fmt.Println("Received RequestVote on srv ", rf.me, "Args", args)
    fmt.Println("RequestVote: On srv ", rf.me, "srv term", rf.currentTerm, " lastLogIdx ", args.LastLogIndex, " lastLogTerm ", rf.log[len(rf.log)-1].Term)

    if args.CandidatesTerm < rf.currentTerm {
        reply.VoteGranted = false
        reply.Term        = rf.currentTerm
        reply.Success     = true
        return
    } else if args.CandidatesTerm == rf.currentTerm {
        if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && (args.LastLogIndex == len(rf.log)-1 && args.LastLogTerm == rf.log[len(rf.log)-1].Term) {
//            rf.status = FOLLOWER
//            rf.electionTimer = time.Now()
            reply.VoteGranted = true
            reply.Term = rf.currentTerm
            reply.Success = true
            return
        } else {
            reply.VoteGranted = false
            reply.Term = rf.currentTerm
            reply.Success = true
            return
        }
    } else {
        rf.currentTerm = args.CandidatesTerm
        rf.votedFor = -1
        rf.status = FOLLOWER
//        rf.electionTimer = time.Now()
        if args.LastLogIndex == len(rf.log)-1 && args.LastLogTerm == rf.log[len(rf.log)-1].Term {
//            rf.currentTerm = args.CandidatesTerm
            rf.votedFor    = args.CandidateId
            rf.electionTimer = time.Now()
//            rf.status = FOLLOWER
            reply.VoteGranted = true
            reply.Term = rf.currentTerm
            reply.Success = true
            return
        } else {
            reply.VoteGranted = false
            reply.Term = rf.currentTerm
            reply.Success = true
            return
        }
    }
}

//Append Entries RPC
type AppendEntriesArgs struct {
    LeadersTerm                     int             //leader's term
    LeaderId                        int             //leader id
    PrevLogIndex                    int             //index of log entry immediately preceding new ones
    PrevLogTerm                     int             //term of PrevLogIndex
    LogEntries                      []LogEntry      //log entries to store
    LeaderCommit                    int             //leader's commit index
    IsEmpty                         bool
}

//AppendEntriesRPCReply
type AppendEntriesReply struct {
    Term                            int             //current term for the leader to update itself
    Success                         bool            //True if follower contained entry matching PrevLogIndexx and PrevLogTerm
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
    // Your code here (2A, 2B). 
    rf.mu.Lock()
    defer rf.mu.Unlock()
    fmt.Println("Received AppendEntries on srv ", rf.me, "Args", args)
	if args.LeadersTerm < rf.currentTerm {
		reply.Term	  = rf.currentTerm
		reply.Success = false
	} else if args.PrevLogIndex > len(rf.log)-1 {
        reply.Term    = rf.currentTerm
        reply.Success = false
    } else if args.PrevLogTerm != rf.log[args.PrevLogIndex].Term {
        reply.Term    = rf.currentTerm
        reply.Success = false
    } else if args.LeadersTerm == rf.currentTerm {
        rf.status     = FOLLOWER
        rf.electionTimer = time.Now()
        reply.Term    = rf.currentTerm
        reply.Success = true
    } else {
        rf.electionTimer = time.Now()
		rf.currentTerm  = args.LeadersTerm
        rf.votedFor     = -1
		rf.status       = FOLLOWER
		reply.Term      = rf.currentTerm
		reply.Success   = true
	}
    return
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
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}


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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	return index, term, isLeader
}

// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

func (rf *Raft) ActAsLeader() {
    fmt.Println("Leader srv", rf.me)
    for {
        rf.mu.Lock()
        if rf.status != LEADER {
            rf.mu.Unlock()
            break
        }

		args := &AppendEntriesArgs {
    		LeadersTerm  : rf.currentTerm             ,
    		LeaderId     : rf.me        			  ,
    		PrevLogIndex : len(rf.log)-1  			  ,
    		PrevLogTerm  : rf.log[len(rf.log)-1].Term ,
    		LeaderCommit : rf.commitIndex			  ,
			IsEmpty		 : true}

	    c := make(chan *AppendEntriesReply)

	    //Send AppendEntry RPCs to everyone
    	for idx := 0; idx < len(rf.peers); idx++ {
        	reply := &AppendEntriesReply{}
            if idx != rf.me {
        	    go func (server int, args *AppendEntriesArgs, reply *AppendEntriesReply, c chan *AppendEntriesReply) {
                	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
                    if !ok {
						reply.Term = -1      //Dummy term
                	    reply.Success = false
                    }
                    c <- reply
        	    }(idx, args, reply, c)
            }
    	}
        rf.mu.Unlock()

        go func() {
    		numRepliesReceived := 0
    		numSuccessReceived := 1
            updateToFollower := false
        	for numRepliesReceived < len(rf.peers)-1 {
    	        reply := <-c
                rf.mu.Lock()
        	    if reply.Success {
        	        numSuccessReceived += 1
    	        } else if reply.Term > rf.currentTerm {
                    rf.currentTerm = reply.Term
                    rf.votedFor    = -1
    				updateToFollower = true
    			}
	            numRepliesReceived += 1
                rf.mu.Unlock()
        	}

            fmt.Println("Leader Heartbeat reply on srv", rf.me, "numRepliesReceived", numRepliesReceived, "numSuccessReceived", numSuccessReceived, "updateToFollower", updateToFollower)

	    	if updateToFollower {
                rf.mu.Lock()
	    		rf.status = FOLLOWER
    			//go rf.ActAsFollower()
                rf.mu.Unlock()
	    		return
    		}
        }()
        time.Sleep(rf.heartbeatTimeout)
    }
    rf.mu.Lock()
    if rf.status == FOLLOWER {
        go rf.ActAsFollower()
    }
    rf.mu.Unlock()

	return
}

func (rf *Raft) ActAsFollower() {
    fmt.Println("Follower srv ", rf.me)
    rf.mu.Lock()
    rf.electionTimer = time.Now()
    rf.mu.Unlock()
    var triggerElection bool = false
	var elapsed time.Duration
	for {
        rf.mu.Lock()
        if rf.status != FOLLOWER {
            rf.mu.Unlock()
            break
        }
		elapsed = time.Since(rf.electionTimer)
		if elapsed > rf.electionTimeout {
            triggerElection = true
            rf.mu.Unlock()
			break
		}
        rf.mu.Unlock()
		time.Sleep(2*time.Millisecond)
	}

    if triggerElection {
        fmt.Println("Follower Timeout on srv ", rf.me)
        rf.mu.Lock()
		rf.status = CANDIDATE
        go rf.ActAsCandidate()
        rf.mu.Unlock()
    }
}

func (rf *Raft) ActAsCandidate() {
    fmt.Println("Candidate srv ", rf.me)
	for {
        rf.mu.Lock()
        if rf.status != CANDIDATE {
            rf.mu.Unlock()
            break
        }
		elapsed := time.Since(rf.electionTimer)
        rf.mu.Unlock()
        ////fmt.Println("Candidate srv ", rf.me, "elapsed", elapsed, "rf.electionTimeout", rf.electionTimeout)
		if elapsed > rf.electionTimeout {
            rf.mu.Lock()
		    //Increment current term
		    rf.currentTerm += 1
		    rf.votedFor = -1

		    //Vote for self 
		    rf.votedFor = rf.me

		    //Reset election timer
		    rf.electionTimer = time.Now()
		    args := &RequestVoteArgs{
		        CandidatesTerm : rf.currentTerm,
        		CandidateId    : rf.me         ,
    		    LastLogIndex   : len(rf.log)-1 ,
	        	LastLogTerm    : rf.log[len(rf.log)-1].Term}
		    c := make(chan *RequestVoteReply)
		    //Send Request Vote RPCs to everyone
		    for idx := 0; idx < len(rf.peers); idx++ {
		        reply := &RequestVoteReply{}
                fmt.Println("Candidate srv ", rf.me, "Sending Request Vote", args, " to srv ", idx)
                if idx != rf.me {
    		        go func (server int, args *RequestVoteArgs, reply *RequestVoteReply, c chan *RequestVoteReply) {
	                    	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
    	                	if !ok {
        	                	reply.Success = false
            	        	}
                	    	c <- reply
    	        	}(idx, args, reply, c)
                }
    		}

            rf.mu.Unlock()

            go func() {
                updateToFollower := false
                numRepliesReceived := 0
                numVotesReceived := 1
    		    for numRepliesReceived < len(rf.peers)-1 {
    		        reply := <-c
                    rf.mu.Lock()
    		        if reply.Success {
    		            if reply.VoteGranted {
        		            numVotesReceived += 1
	    	            } else if reply.Term > rf.currentTerm  {    //Update to follower with term received in reply
                            rf.currentTerm = reply.Term
                            rf.votedFor    = -1
	                	    updateToFollower = true
	            	    }
	        	    }
    	        	numRepliesReceived += 1
                    rf.mu.Unlock()
    	    	}
                fmt.Println("Leader election on srv ", rf.me, "numRepliesReceived", numRepliesReceived, "numVotesReceived", numVotesReceived, "updateToFollower", updateToFollower)
                rf.mu.Lock()
                if rf.status == CANDIDATE {
        		    if updateToFollower {
    		    		rf.status = FOLLOWER
    		            //go rf.ActAsFollower()
        		    } else if numVotesReceived > len(rf.peers) / 2 {
			        	rf.status = LEADER
		                go rf.ActAsLeader()
                    }
                }
                rf.mu.Unlock()
                return
            }()
		}

        time.Sleep(10*time.Millisecond)
        rf.mu.Lock()
		elapsed = time.Since(rf.electionTimer)
        rf.mu.Unlock()
	}
    rf.mu.Lock()
	if rf.status == FOLLOWER {
		go rf.ActAsFollower()
	}
    rf.mu.Unlock()
	return
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

    rf.mu.Lock()
    defer rf.mu.Unlock()
	// Your initialization code here (2A, 2B, 2C).
    rf.currentTerm = 0
    rf.votedFor    = -1

	// initialize from state persisted before a crash
	// rf.readPersist(persister.ReadRaftState())

    var v interface{}
    lg := LogEntry{
		Term : 0,
		Log  : v}
    rf.log = append(rf.log, lg)

    s := rand.NewSource(time.Now().UnixNano())
    r := rand.New(s)
    tout := time.Duration(r.Intn(400) + 500)
    rf.electionTimeout = tout * time.Millisecond
    rf.heartbeatTimeout = 100 * time.Millisecond

    rf.status = FOLLOWER
    fmt.Println("Starting server", "me", rf.me, "log", rf.log, "electionTout", rf.electionTimeout, "heartbeatTout", rf.heartbeatTimeout, "status", rf.status)
    go rf.ActAsFollower()

	return rf
}
