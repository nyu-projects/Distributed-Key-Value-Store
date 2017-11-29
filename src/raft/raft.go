package raft

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

import "sync"
import "labrpc"
import "time"
import "math/rand"
//import "math"
import "fmt"
//import "strings"
import "bytes"
import "encoding/gob"

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
    Term        int
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
    nu        sync.Mutex          // Lock to protect applyCh
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
    applyChan           chan ApplyMsg
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
    //
    //logId               int64
    //persistent state on all servers
    currentTerm         int             //latest term server has seen
    votedFor            int             //candidateId that received vote in the current term
    log                 []LogEntry      //log entries. each entry has command and term when entry was received by leader
    indexOffset         int             //
    prevLogIdx          int             //
    prevLogTerm         int             //

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
	heartbeatTimer		time.Time		//leader's timer for sending heartbeat
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

func getActualIdx(idx int, offset int) int {
    return idx - (offset + 1)
}

func getOffsetIdx(idx int, offset int) int {
    return idx + (offset + 1)
}

func (rf *Raft) GetLogAtIndex(index int) (ApplyMsg, bool, bool) {
    rf.mu.Lock()
	fmt.Println("GetLogAtIndex For idx:", index, "Actual idx:", getActualIdx(index, rf.indexOffset))
	var appMsg ApplyMsg
    if rf.lastApplied < index {
		rf.mu.Unlock()
		return appMsg, false, false
    } else if getActualIdx(index, rf.indexOffset) < 0 {
		rf.mu.Unlock()
		return appMsg, false, true
	}
    appMsg = ApplyMsg {
		Index       : index,
        Command     : rf.log[getActualIdx(index, rf.indexOffset)].Log,
        Term        : rf.log[getActualIdx(index, rf.indexOffset)].Term}
    rf.mu.Unlock()
    return appMsg, true, false
}

func (rf *Raft) DiscardOldLogs(LastIncIdx int, LastIncTerm int) {
    rf.mu.Lock()
    defer rf.mu.Unlock()
	fmt.Println("Discarding Old Log on srv:", rf.me, " till index:", LastIncIdx, "cutIdx:", LastIncIdx - rf.indexOffset)
    cutIdx := LastIncIdx - rf.indexOffset   //120 - 100
    // 121 122 123 ... 140
    // 101 102 103 ... 120
    // 0   1   2   ... 19
    //cutIdx = 120 - 100 = 20
	if cutIdx > 0 && cutIdx < len(rf.log) {
        rf.prevLogIdx = LastIncIdx
        rf.prevLogTerm  = LastIncTerm
		fmt.Println("Discarding Old Log on srv:", rf.me, "Before length:", rf.log, "Old IndexOffset", rf.indexOffset, "cutIdx:", cutIdx)
		rf.indexOffset = LastIncIdx
    	rf.log = rf.log[cutIdx:]
		fmt.Println("Discarding Old Log on srv:", rf.me, "After length:", rf.log, "New IndexOffset", rf.indexOffset)
    	rf.persist()
	}
    return
}

type PersistData struct {
    CurrentTerm     int
    VotedFor        int
    IndexOffset     int
    PrevLogIdx      int
    PrevLogTerm     int
    Log             []LogEntry
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
    pData := PersistData{rf.currentTerm, rf.votedFor, rf.indexOffset, rf.prevLogIdx, rf.prevLogTerm, rf.log}
	e.Encode(pData)
    fmt.Println("Raft Persist: srv", rf.me, " pData ", len(pData.Log))
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) bool {
	// Your code here (2C).
	var pData PersistData
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&pData)
	if (data == nil || len(data) < 1) {
		return false
	}
    fmt.Println("Read Raft Persist: srv", rf.me, " pData ", len(pData.Log))
    rf.currentTerm  = pData.CurrentTerm
    rf.votedFor     = pData.VotedFor
    rf.indexOffset  = pData.IndexOffset
    rf.prevLogIdx   = pData.PrevLogIdx
    rf.prevLogTerm  = pData.PrevLogTerm
    rf.log = make([]LogEntry, len(pData.Log))
    copy(rf.log, pData.Log)
    return true
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
	//Success							bool		//Call Succeeded
    Term                            int         //current Term
    VoteGranted                     bool        //candidate received vote or not
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
    rf.mu.Lock()
    defer rf.mu.Unlock()
    fmt.Println("Received RequestVote on srv ", rf.me, "Args", args)
    fmt.Println("RequestVote: On srv ", rf.me, "Args", args, "srv term", rf.currentTerm, " lastLogIdx ", len(rf.log)-1)

    if args.CandidatesTerm < rf.currentTerm {
        reply.VoteGranted = false
        reply.Term        = rf.currentTerm
        fmt.Println("RequestVote: On srv ", rf.me, "inside 1")
        return
    } else if args.CandidatesTerm == rf.currentTerm {
		var grantVote bool
		ActualLastLogIndex := getActualIdx(args.LastLogIndex, rf.indexOffset)
		if len(rf.log) > 0 {
        	grantVote = ((args.LastLogTerm > rf.log[len(rf.log)-1].Term) || ((args.LastLogTerm == rf.log[len(rf.log)-1].Term) && (ActualLastLogIndex >= len(rf.log)-1)))
		} else {
            //_, _, snapshot := rf.readSnapshotPersist()
            //prevLogIdx  := snapshot.LastIncIdx
            //prevLogTerm := snapshot.LastIncTerm

			grantVote = ((args.LastLogTerm > rf.prevLogTerm) || ((args.LastLogTerm == rf.prevLogTerm) && (args.LastLogIndex >= rf.prevLogIdx)))
		}
        if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && (grantVote) {
            rf.status = FOLLOWER
            rf.electionTimer = time.Now()
            rf.votedFor = args.CandidateId
            rf.persist()
            reply.VoteGranted = true
            reply.Term = rf.currentTerm
            fmt.Println("RequestVote: On srv ", rf.me, "inside 2")
            return
        } else {
            reply.VoteGranted = false
            reply.Term = rf.currentTerm
            fmt.Println("RequestVote: On srv ", rf.me, "inside 3")
            return
        }
    } else {
        rf.currentTerm = args.CandidatesTerm
        rf.votedFor = -1
        rf.status = FOLLOWER

        var grantVote bool
        ActualLastLogIndex := getActualIdx(args.LastLogIndex, rf.indexOffset)
        if len(rf.log) > 0 {
            grantVote = ((args.LastLogTerm > rf.log[len(rf.log)-1].Term) || ((args.LastLogTerm == rf.log[len(rf.log)-1].Term) && (ActualLastLogIndex >= len(rf.log)-1)))
        } else {
            //_, _, snapshot := rf.readSnapshotPersist()
            //prevLogIdx  := snapshot.LastIncIdx
            //prevLogTerm := snapshot.LastIncTerm

            grantVote = ((args.LastLogTerm > rf.prevLogTerm) || ((args.LastLogTerm == rf.prevLogTerm) && (args.LastLogIndex >= rf.prevLogIdx)))
        }

        if grantVote {
            rf.votedFor    = args.CandidateId
            rf.electionTimer = time.Now()
            rf.persist()
            reply.VoteGranted = true
            reply.Term = rf.currentTerm
            fmt.Println("RequestVote: On srv ", rf.me, "inside 4")
            return
        } else {
            rf.persist()
            reply.VoteGranted = false
            reply.Term = rf.currentTerm
            fmt.Println("RequestVote: On srv ", rf.me, "inside 5")
            return
        }
    }
}

func Min(x, y int) int {
    if x < y {
        return x
    }
    return y
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
    Term                            int             //current term for the leader to update itsel
    ConflictEntryIdx                int
    ConflictTerm                    int
    Success                         bool            //True if follower contained entry matching PrevLogIndexx and PrevLogTerm
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
    // Your code here (2A, 2B). 
    rf.mu.Lock()
    defer rf.mu.Unlock()
	fmt.Println("AppendEntries srv:", rf.me, "Args:", args, "prevLogIndex:", args.PrevLogIndex, "indexOffset: ", rf.indexOffset)
    ActualPrevLogIndex := getActualIdx(args.PrevLogIndex, rf.indexOffset)
    fmt.Println("AppendEntries srv:", rf.me, "Args:", args, "rf.currentTerm:", rf.currentTerm, "ActualPrevLogIndex: ", ActualPrevLogIndex)
	if args.LeadersTerm < rf.currentTerm {
        fmt.Println("AppendEntries srv:", rf.me, "Case 1")
		reply.Term	  = rf.currentTerm
		reply.Success = false
	} else if ActualPrevLogIndex > len(rf.log)-1 {
        fmt.Println("AppendEntries srv:", rf.me, "Case 2")
        //
        if args.LeadersTerm > rf.currentTerm {
            rf.votedFor = -1
			rf.currentTerm  = args.LeadersTerm
			rf.persist()
        }
        rf.status     = FOLLOWER
        rf.electionTimer = time.Now()
        //
        reply.ConflictEntryIdx = getOffsetIdx(len(rf.log), rf.indexOffset)
        reply.ConflictTerm	   = -1
        reply.Term    = rf.currentTerm
        reply.Success = false
    } else if ActualPrevLogIndex < 0 {
		fmt.Println("AppendEntries srv:", rf.me, "Case ActualPrevLogIndex < 0")
        if args.LeadersTerm > rf.currentTerm {
            rf.votedFor = -1
	        rf.status     = FOLLOWER
    	    rf.electionTimer = time.Now()
        	rf.currentTerm  = args.LeadersTerm
        	rf.persist()
        }

		if ActualPrevLogIndex == -1 {
        	//_, _, snapshot := rf.readSnapshotPersist()
           	//prevLogIdx  := snapshot.LastIncIdx
           	//prevLogTerm := snapshot.LastIncTerm
			if rf.prevLogTerm == args.PrevLogTerm {
				rf.status     = FOLLOWER
				rf.electionTimer = time.Now()

		        if !args.IsEmpty {
		            fmt.Println("AppendEntries srv:", rf.me, "After snapshot: before log", rf.log)
					if len(rf.log) == 0 {
						rf.log = append(rf.log, args.LogEntries...)
					} else {
    			        replaceLogs := false
        			    logIdx := ActualPrevLogIndex+1
			            argsIdx := 0
	    		        for logIdx < len(rf.log) && argsIdx < len(args.LogEntries) {
        			        if rf.log[logIdx].Term != args.LogEntries[argsIdx].Term {
    	        		        replaceLogs = true
	                		    break
	        	        	}
	    		            logIdx  += 1
    		    	        argsIdx += 1
		    	        }
		        	    if replaceLogs || argsIdx < len(args.LogEntries) {
    			            rf.log = rf.log[:logIdx]
        			        args.LogEntries = args.LogEntries[argsIdx:]
    	        		    rf.log = append(rf.log, args.LogEntries...)
			            }
					}
    		        fmt.Println("AppendEntries srv:", rf.me, "After snapshot: after log", rf.log)
        		}

          		rf.persist()

        tentativeCommitIndex := Min(args.LeaderCommit, getOffsetIdx(len(rf.log)-1, rf.indexOffset))
        if tentativeCommitIndex > rf.commitIndex {
            rf.commitIndex = tentativeCommitIndex
        }
        fmt.Println("AppendEntries srv:", rf.me, "After snapshot: updated commitIdx to:", rf.commitIndex)
        go func() {
            fmt.Println("AppendEntries srv:", rf.me, "After snapshot Sending all appmsgs. commitIndex", rf.commitIndex, "lastApplied", rf.lastApplied)
            for {
                rf.nu.Lock()
                rf.mu.Lock()
                if rf.lastApplied == rf.commitIndex {
                    rf.mu.Unlock()
                    rf.nu.Unlock()
                    break
                }
                rf.lastApplied += 1
                fmt.Println("AppendEntries srv:", rf.me, "After snapshot Sending ApplyMsg", "commitIndex", rf.commitIndex, "lastApplied", rf.lastApplied, "rf.log", rf.log)
                fmt.Println("AppendEntries srv:", rf.me, "After snapshot Sending ApplyMsg", "actualIdx", getActualIdx(rf.lastApplied, rf.indexOffset), "log length:", len(rf.log))
                appMsg := ApplyMsg {
                    Index       : rf.lastApplied,
                    Command     : rf.log[getActualIdx(rf.lastApplied, rf.indexOffset)].Log,
                    Term        : rf.log[getActualIdx(rf.lastApplied, rf.indexOffset)].Term}
                rf.mu.Unlock()
                fmt.Println("AppendEntries srv:", rf.me, "After snapshot Sending ApplyMsg", appMsg)
                rf.applyChan <- appMsg
                rf.nu.Unlock()
            }
            fmt.Println("AppendEntries srv:", rf.me, "After snapshot Sent all appmsgs. commitIndex", rf.commitIndex, "lastApplied", rf.lastApplied)
        }()


				reply.Term		= rf.currentTerm
				reply.Success   = true
			} else {
				reply.Term	  = rf.currentTerm
				reply.Success = false
			}
		} else {
			reply.Term		= rf.currentTerm
			reply.Success   = false
		}
	} else if args.PrevLogTerm != rf.log[ActualPrevLogIndex].Term {
        fmt.Println("AppendEntries srv:", rf.me, "Case 3")
        //
        if args.LeadersTerm > rf.currentTerm {
            rf.votedFor = -1
            rf.currentTerm  = args.LeadersTerm
            rf.persist()
        }
        rf.status     = FOLLOWER
        rf.electionTimer = time.Now()

        firstIdxWithTerm := ActualPrevLogIndex
        for firstIdxWithTerm > 0 {
            if rf.log[firstIdxWithTerm-1].Term != rf.log[firstIdxWithTerm].Term {
                break
            }
            firstIdxWithTerm -= 1
        }

		if firstIdxWithTerm == 0 && len(rf.log) > 1 {
			if rf.log[firstIdxWithTerm].Term == rf.log[firstIdxWithTerm+1].Term {
				firstIdxWithTerm -= 1
			}
		}

        reply.ConflictEntryIdx = getOffsetIdx(firstIdxWithTerm, rf.indexOffset)
        reply.ConflictTerm = rf.log[ActualPrevLogIndex].Term
        reply.Term    = rf.currentTerm
        reply.Success = false
    } else {
        fmt.Println("AppendEntries srv:", rf.me, "Case 4")
        if args.LeadersTerm > rf.currentTerm {
            rf.votedFor = -1
        }
        rf.status     = FOLLOWER
        rf.electionTimer = time.Now()
        rf.currentTerm  = args.LeadersTerm

        if !args.IsEmpty {
			fmt.Println("AppendEntries srv:", rf.me, "before log", len(rf.log))
			replaceLogs := false
			logIdx := ActualPrevLogIndex+1
			argsIdx := 0
			for logIdx < len(rf.log) && argsIdx < len(args.LogEntries) {
				if rf.log[logIdx].Term != args.LogEntries[argsIdx].Term {
					replaceLogs = true
					break
				}
				logIdx  += 1
				argsIdx += 1
			}
			if replaceLogs || argsIdx < len(args.LogEntries) {
				rf.log = rf.log[:logIdx]
				args.LogEntries = args.LogEntries[argsIdx:]
				rf.log = append(rf.log, args.LogEntries...)
			}
			fmt.Println("AppendEntries srv:", rf.me, "after log", len(rf.log))
        }

        tentativeCommitIndex := Min(args.LeaderCommit, getOffsetIdx(len(rf.log)-1, rf.indexOffset))
		if tentativeCommitIndex > rf.commitIndex {
			rf.commitIndex = tentativeCommitIndex
		}

		fmt.Println("AppendEntries srv:", rf.me, "commitIndex", rf.commitIndex, "lastApplied", rf.lastApplied)
        go func() {
            for {
                rf.nu.Lock()
                rf.mu.Lock()
                if rf.lastApplied == rf.commitIndex {
                    rf.mu.Unlock()
                    rf.nu.Unlock()
                    break
                }
                rf.lastApplied += 1
                appMsg := ApplyMsg {
                    Index       : rf.lastApplied,
                    Command     : rf.log[getActualIdx(rf.lastApplied, rf.indexOffset)].Log,
                    Term        : rf.log[getActualIdx(rf.lastApplied, rf.indexOffset)].Term}
                rf.mu.Unlock()
                fmt.Println("AppendEntries srv:", rf.me, "Sending ApplyMsg", appMsg)
                rf.applyChan <- appMsg
				rf.nu.Unlock()
            }
            fmt.Println("AppendEntries srv:", rf.me, "Sent all appmsgs. commitIndex", rf.commitIndex, "lastApplied", rf.lastApplied)
        }()

        rf.persist()
        reply.Term    = rf.currentTerm
        reply.Success = true
    }
    return
}

type InstallSnapshotArgs struct {
    LeadersTerm                     int
    LeaderId                        int
    LastIncludedIndex               int
    LastIncludedTerm                int
//    ByteOffset                    int
    Data                            []byte
//    Done                            bool
}

type InstallSnapshotReply struct {
    Term                            int             //current term for the leader to update itself
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
    rf.mu.Lock()
	fmt.Println("InstallSnapshot on srv:", rf.me, args.LeadersTerm, args.LeaderId, args.LastIncludedIndex, args.LastIncludedTerm)
    if args.LeadersTerm < rf.currentTerm {
        reply.Term = rf.currentTerm
		rf.mu.Unlock()
        return
    } else if args.LeadersTerm > rf.currentTerm {
        rf.votedFor = -1
        rf.status     = FOLLOWER
        rf.electionTimer = time.Now()
        rf.currentTerm  = args.LeadersTerm
	}
/*
	if len(rf.log) > 0 {
    	logIdx := 0
		cutLog := false
        for logIdx < len(rf.log) {
        	if getOffsetIdx(logIdx, rf.indexOffset) == args.LastIncludedIndex && rf.log[logIdx].Term == args.LastIncludedTerm {
				cutLog = true
            	break
            }
            logIdx  += 1
        }

		if cutLog {
			rf.log = rf.log[logIdx+1:]
			rf.indexOffset += logIdx
		} else {
			rf.log = rf.log[:0]
		}

		rf.commitIndex = rf.indexOffset
		rf.lastApplied = rf.indexOffset
	}
*/
    if rf.prevLogIdx >= args.LastIncludedIndex {
        reply.Term = rf.currentTerm
        rf.mu.Unlock()
        return
    }
    rf.prevLogIdx   = args.LastIncludedIndex
    rf.prevLogTerm  = args.LastIncludedTerm
	rf.log = rf.log[:0]
	rf.indexOffset  = args.LastIncludedIndex

	rf.commitIndex = rf.indexOffset
	rf.lastApplied = rf.indexOffset

    appMsg := ApplyMsg {
   		UseSnapshot:	true,
    	Snapshot  :		args.Data}

	fmt.Println("InstallSnapshot on srv:", rf.me, "Updated rf.log", rf.log, "indexOffset:", rf.indexOffset, "rf.commitIndex: ", rf.commitIndex, "rf.lastApplied:", rf.lastApplied)

	rf.persist()
	reply.Term = rf.currentTerm
	rf.mu.Unlock()

	rf.applyChan <- appMsg

	return
}


func (rf *Raft) UpdateRaftState(LastIncludedIndex int, LastIncludedTerm int) {
	rf.mu.Lock()

    logIdx := 0

    for logIdx < len(rf.log) {
        if getOffsetIdx(logIdx, rf.indexOffset) <= LastIncludedIndex {
			logIdx += 1
        } else {
			break
		}
    }

    if logIdx < len(rf.log) {
        rf.log = rf.log[logIdx:]
        rf.indexOffset += logIdx
    } else {
        rf.log = rf.log[:0]
		rf.indexOffset += logIdx
    }

	rf.commitIndex = rf.indexOffset
	rf.lastApplied = rf.indexOffset
    if rf.commitIndex == -1 && rf.lastApplied == -1 {
        rf.commitIndex = 0
        rf.lastApplied = 0
    }
	rf.persist()
	rf.mu.Unlock()
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

type PersistSnapshotData struct {
    LastIncIdx      int
    LastIncTerm     int
    KvMap           map[string][]string
    ClientReqMap    map[int64]int64
}

func (rf *Raft) readSnapshotPersist() (bool, []byte, *PersistSnapshotData) {
	var data []byte
	data = rf.persister.ReadSnapshot()
    var pData PersistSnapshotData
    r := bytes.NewBuffer(data)
    d := gob.NewDecoder(r)
    d.Decode(&pData)
    if data == nil || len(data) < 1 {
        return false, data, &pData
    }
	fmt.Println("Reading snapshot on raft srv:", rf.me)
    return true, data, &pData
}


func (rf *Raft) Start(command interface{}) (int, int, bool) {
    rf.mu.Lock()
    defer rf.mu.Unlock()

    if rf.status != LEADER {
        return -1, -1, false
    }
    currentEntry := LogEntry{
                        Log  : command,
                        Term : rf.currentTerm}

    rf.log = append(rf.log, currentEntry)
    fmt.Println("Start srv", rf.me, "Received command: ", command, "Current Log", len(rf.log), "commitIdx", rf.commitIndex, "lastApplied", rf.lastApplied, "ndexOffset:", rf.indexOffset)
	index := getOffsetIdx(len(rf.log)-1, rf.indexOffset)
	term := rf.currentTerm
	isLeader := true
    rf.persist()

	go func() {
		    rf.mu.Lock()
		    rf.heartbeatTimer = time.Now()
		    rf.mu.Unlock()
		
        	AppendEntriesChannel := make(chan *AppendEntriesReply, len(rf.peers)-1)
	        //Send AppendEntry RPCs to everyone
    	    for idx := 0; idx < len(rf.peers); idx++ {
        	    if idx != rf.me {
            	    go func (server int, currentTerm int) {
						for {
							rf.mu.Lock()
							if rf.status != LEADER {
								rf.mu.Unlock()
								break
							}

							srvNxtIdx := getActualIdx(rf.nextIndex[server], rf.indexOffset)
							fmt.Println("Start srv", rf.me, "Server: ", server, "rf.nextIndex[server]:", rf.nextIndex[server], "indexOffset:", rf.indexOffset, "srvNxtIdx:", srvNxtIdx)
							if srvNxtIdx < 0 {
								fmt.Println("Start: srv", rf.me, "Entering InstallSnapshot for server", server, "srvNxtIdx: ", srvNxtIdx)
								var wg sync.WaitGroup
								wg.Add(1)
								go func(leaderTerm int) {
									rf.mu.Lock()

                                    ok, data, snapshot := rf.readSnapshotPersist()

									if ok {
										installargs := &InstallSnapshotArgs{
												LeadersTerm			: leaderTerm,
												LeaderId			: rf.me,
												LastIncludedIndex	: snapshot.LastIncIdx,
												LastIncludedTerm	: snapshot.LastIncTerm,
												Data				: data}

			                            installreply := &InstallSnapshotReply{}
										srvNextIdx := getOffsetIdx(0, rf.indexOffset)
			                            fmt.Println("Start srv", rf.me, "Sending InstallSnapshot to srv", server)
										rf.mu.Unlock()
            			                ok := rf.peers[server].Call("Raft.InstallSnapshot", installargs, installreply)
                            			rf.mu.Lock()

                            			fmt.Println("Start srv", rf.me, "Received Snapshot reply from srv", server, "reply", installreply)
                                        fmt.Println("Start srv", rf.me, "Snapshot reply from srv", server, "ok", ok, "leaderTerm", leaderTerm, "rf.currentTerm", rf.currentTerm, "reply.Term", installreply.Term)
                            			if ok && rf.currentTerm == leaderTerm {
                                			if installreply.Term <= leaderTerm {
												if srvNextIdx > rf.nextIndex[server] {
													rf.nextIndex[server] = srvNextIdx
													rf.matchIndex[server] = rf.nextIndex[server] - 1
                                                    fmt.Println("Start srv", rf.me, "InstallSnapshotReply from srv", server, "Incremented nextIndex:", rf.nextIndex[server])
												}
		                                	} else if installreply.Term > leaderTerm {
							                    rf.status = FOLLOWER
                    							rf.currentTerm = installreply.Term
                    							rf.votedFor = -1
                    							rf.persist()
                                                fmt.Println("Start srv", rf.me, "InstallSnapshotReply from srv", server, "Becoming follower")
		                                	}
			                       		} else if ok && installreply.Term > rf.currentTerm {
                                            rf.status = FOLLOWER
                                            rf.currentTerm = installreply.Term
                                            rf.votedFor = -1
                                            rf.persist()
                                            fmt.Println("Start srv", rf.me, "InstallSnapshotReply from srv", server, "Becoming follower")
            			           		}
									}
									rf.mu.Unlock()
									wg.Done()

								}(currentTerm)

								rf.mu.Unlock()
								wg.Wait()
								rf.mu.Lock()
							}

                            if rf.status != LEADER {
                                rf.mu.Unlock()
                                break
                            }

                            var logEntryArray []LogEntry
                            currNextIdx := rf.nextIndex[server]
                            logIdx := getActualIdx(rf.nextIndex[server], rf.indexOffset)

							if logIdx < 0 {
								rf.mu.Unlock()
								continue
							}

							for logIdx < len(rf.log) {
								logEntryArray = append(logEntryArray, rf.log[logIdx])
								logIdx += 1
							}

							pLogIdx := Min(rf.nextIndex[server]-1, getOffsetIdx(len(rf.log)-1, rf.indexOffset))
                            if pLogIdx < 0 {
                                pLogIdx = 0
                            }

							var pLogTerm int
							if getActualIdx(pLogIdx, rf.indexOffset) < 0 {
								//_, _, snapshot := rf.readSnapshotPersist()
								//prevLogIdx  = snapshot.LastIncIdx
								//prevLogTerm = snapshot.LastIncTerm
                                pLogIdx  = rf.prevLogIdx
                                pLogTerm = rf.prevLogTerm
								fmt.Println("Start srv", rf.me, "Reading previdx and prevterm from snapshot")
							} else {
								pLogTerm = rf.log[getActualIdx(pLogIdx, rf.indexOffset)].Term
							}

                            fmt.Println("Start: srv", rf.me, "For server", server, "PrevLogIndex", pLogIdx, "PrevLogTerm", pLogTerm, "rf.log", len(rf.log))

						    args := &AppendEntriesArgs {
					            LeadersTerm  : currentTerm                ,
					            LeaderId     : rf.me                      ,
					            PrevLogIndex : pLogIdx				      ,
					            PrevLogTerm  : pLogTerm				      ,
					            LogEntries   : logEntryArray              ,
					            LeaderCommit : rf.commitIndex             ,
					            IsEmpty      : len(logEntryArray)==0}

							rf.mu.Unlock()
                	    	reply := &AppendEntriesReply{}
                            fmt.Println("Start srv", rf.me, "Sending AppendEntries to srv", server, "args", args)
                    		ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
							rf.mu.Lock()
                            fmt.Println("Start srv", rf.me, "Received reply from srv", server, "reply", reply)
	                    	if ok && rf.currentTerm == currentTerm {
								if reply.Success {
                                    if currNextIdx + len(logEntryArray) > rf.nextIndex[server] {
                                        rf.nextIndex[server] = currNextIdx + len(logEntryArray)
                                    }
                                    if rf.nextIndex[server] > getOffsetIdx(len(rf.log), rf.indexOffset) {
                                        rf.nextIndex[server] = getOffsetIdx(len(rf.log), rf.indexOffset)
                                    }

									rf.matchIndex[server]   = rf.nextIndex[server] - 1
									AppendEntriesChannel <- reply
									fmt.Println("Start srv", rf.me, "Breaking off for srv", server)
									rf.mu.Unlock()
									break
								} else if reply.Term > currentTerm {
                                    AppendEntriesChannel <- reply
									rf.mu.Unlock()
									fmt.Println("Start srv", rf.me, "Follower ", server, "has higher term. Breaking off")
                                    break
                                } else {
									rf.nextIndex[server]   = reply.ConflictEntryIdx
									fmt.Println("Start srv", rf.me, "Decrementing nextIdx for srv", server, "nextIdx", rf.nextIndex[server])
									if rf.nextIndex[server] < 1 {
										rf.nextIndex[server] = 1
									}
                                }
                            } else if ok && reply.Term > rf.currentTerm {
                                AppendEntriesChannel <- reply
								fmt.Println("Start srv", rf.me, "Append Entry reply from srv", server, "reply.Term", reply.Term, "rf.currentTerm", rf.currentTerm)
                                rf.mu.Unlock()
                                break
                            } else {
								fmt.Println("Start srv", rf.me, "Append Entry reply from srv", server, "NILL reply")
                                AppendEntriesChannel <- nil
                                rf.mu.Unlock()
                                break
                            }
							rf.mu.Unlock()
						}
						return
    	            }(idx, term)
	            }
    	    }
	        go func() {
    	        numRepliesReceived := 0
        	    numSuccessReceived := 1
            	updateToFollower := false
	            rf.mu.Lock()
    	        highestTermSeen := rf.currentTerm
        	    rf.mu.Unlock()
	            for reply := range AppendEntriesChannel {
    	            numRepliesReceived += 1
        	        if reply != nil {
            	        if reply.Success {
                	        numSuccessReceived += 1
                    	} else if reply.Term > highestTermSeen {
	                        highestTermSeen = reply.Term
    	                    updateToFollower = true
        	            }
            	    }
	                if numRepliesReceived == len(rf.peers)-1 || numSuccessReceived > len(rf.peers) / 2 || updateToFollower {
    	                break
        	        }
            	}
                fmt.Println("Start srv", rf.me, "Collecting replies", "numRepliesReceived", numRepliesReceived, "numSuccessReceived", numSuccessReceived, "updateToFollower", updateToFollower)
	            rf.mu.Lock()
    	        if updateToFollower && rf.currentTerm < highestTermSeen {
            	    rf.status = FOLLOWER
        	        rf.currentTerm = highestTermSeen
                	rf.votedFor = -1
                    rf.persist()
	                fmt.Println("Start srv", rf.me, "Collector thread: srv", rf.me, "Found a srv with higher term")
            	} else if rf.status == LEADER && numSuccessReceived > len(rf.peers) / 2 {
                    if (index > rf.commitIndex) && (index < getOffsetIdx(len(rf.log), rf.indexOffset)) {
                       rf.commitIndex = index
                    }
					fmt.Println("Start srv", rf.me, "Collector thread: Sending ApplyMsg", "commitIdx", rf.commitIndex, "lastApplied", rf.lastApplied)
		            go func() {
						fmt.Println("Start srv", rf.me, "ApplyMsgSender thread: ", "rf.log", len(rf.log), "commitIdx", rf.commitIndex, "lastApplied", rf.lastApplied)
                		for {
                            rf.nu.Lock()
        		            rf.mu.Lock()
		                    if rf.lastApplied == rf.commitIndex {
                        		rf.mu.Unlock()
                                rf.nu.Unlock()
                		        break
        		            }
		                    rf.lastApplied += 1
							fmt.Println("Start srv", rf.me, "ApplyMsgSender thread: ", "commitIdx", rf.commitIndex, "lastApplied", rf.lastApplied, "rf.log", len(rf.log))
		                    appMsg := ApplyMsg {
        		                Index       : rf.lastApplied,
                		        Command     : rf.log[getActualIdx(rf.lastApplied, rf.indexOffset)].Log,
                                Term        : rf.log[getActualIdx(rf.lastApplied, rf.indexOffset)].Term}

                            fmt.Println("Start srv", rf.me, "ApplyMsgSender thread: Sending ApplyMsg", appMsg)
                            rf.mu.Unlock()
                		    rf.applyChan <- appMsg
							rf.nu.Unlock()
        		        }
                        fmt.Println("Start srv", rf.me, "ApplyMsgSender thread: Sent all ApplyMsg", "commitIdx", rf.commitIndex, "lastApplied", rf.lastApplied)
		            }()
                }
	            rf.mu.Unlock()
    	        return
        	}()
    }()
	// Your code here (2B).
	return index, term, isLeader
}

// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (rf *Raft) Kill() {
	// Your code here, if desired.
    rf.mu.Lock()
    rf.status = -1
    rf.mu.Unlock()
}


func (rf *Raft) ActAsLeader() {
    rf.mu.Lock()
    fmt.Println("Leader srv", rf.me, "rf.log", len(rf.log), "rf.currentTerm", rf.currentTerm, "commitIdx", rf.commitIndex, "lastApplied", rf.lastApplied)
    for idx := 0; idx < len(rf.peers); idx++ {
        if idx != rf.me {
            rf.nextIndex[idx]  = getOffsetIdx(len(rf.log), rf.indexOffset)
            rf.matchIndex[idx] = 0
        } else {
            rf.nextIndex[idx]  = getOffsetIdx(len(rf.log), rf.indexOffset)
            rf.matchIndex[idx] = getOffsetIdx(len(rf.log)-1, rf.indexOffset)
        }
    }
	rf.mu.Unlock()
    for {
        rf.mu.Lock()
        if rf.status != LEADER {
            rf.mu.Unlock()
            break
        }
        elapsed := time.Since(rf.heartbeatTimer)
		rf.mu.Unlock()

        if elapsed > rf.heartbeatTimeout {
			rf.mu.Lock()
			rf.heartbeatTimer = time.Now()
		    AppendEntriesChannel := make(chan *AppendEntriesReply, len(rf.peers)-1)
		    //Send AppendEntry RPCs to everyone
    		for idx := 0; idx < len(rf.peers); idx++ {
        	    if idx != rf.me {
                    go func (server int, currentTerm int) {
                        for {
                            rf.mu.Lock()
                            if rf.status != LEADER{
                                rf.mu.Unlock()
                                break
                            }

                            srvNxtIdx := getActualIdx(rf.nextIndex[server], rf.indexOffset)

                            if srvNxtIdx < 0 {
                                var wg sync.WaitGroup
                                wg.Add(1)
                                go func(leaderTerm int) {
                                    rf.mu.Lock()

                                    ok, data, snapshot := rf.readSnapshotPersist()

                                    if ok {
                                        installargs := &InstallSnapshotArgs{
                                                LeadersTerm         : leaderTerm,
                                                LeaderId            : rf.me,
                                                LastIncludedIndex   : snapshot.LastIncIdx,
                                                LastIncludedTerm    : snapshot.LastIncTerm,
                                                Data                : data}

                                        installreply := &InstallSnapshotReply{}
                                        srvNextIdx := getOffsetIdx(0, rf.indexOffset)
                                        fmt.Println("Leader srv", rf.me, "Sending InstallSnapshot to srv", server)
                                        rf.mu.Unlock()
                                        ok := rf.peers[server].Call("Raft.InstallSnapshot", installargs, installreply)
                                        rf.mu.Lock()
                                        fmt.Println("Leader srv", rf.me, "Received Snapshot reply from srv", server, "reply", installreply)
                                        fmt.Println("Leader srv", rf.me, "Snapshot reply from srv", server, "ok", ok, "leaderTerm", leaderTerm, "rf.currentTerm", rf.currentTerm, "reply.Term", installreply.Term)
                                        if ok && rf.currentTerm == leaderTerm {
                                            if installreply.Term <= leaderTerm {
                                                if srvNextIdx > rf.nextIndex[server] {
                                                    rf.nextIndex[server] = srvNextIdx
                                                    rf.matchIndex[server] = rf.nextIndex[server] - 1
                                                    fmt.Println("Leader srv", rf.me, "InstallSnapshotReply from srv", server, "Incremented nextIndex:", rf.nextIndex[server])
                                                }
                                            } else if installreply.Term > leaderTerm {
                                                rf.status = FOLLOWER
                                                rf.currentTerm = installreply.Term
                                                rf.votedFor = -1
                                                rf.persist()
                                                fmt.Println("Leader srv", rf.me, "InstallSnapshotReply from srv", server, "Becoming follower")
                                            }
                                        } else if ok && installreply.Term > rf.currentTerm {
                                            rf.status = FOLLOWER
                                            rf.currentTerm = installreply.Term
                                            rf.votedFor = -1
                                            rf.persist()
                                            fmt.Println("Leader srv", rf.me, "InstallSnapshotReply from srv", server, "Becoming follower")
                                        }
                                    }
									rf.mu.Unlock()
                                    wg.Done()

                                }(currentTerm)

								rf.mu.Unlock()
                                wg.Wait()
								rf.mu.Lock()
                            }

                            if rf.status != LEADER {
                                rf.mu.Unlock()
                                break
                            }

                            var logEntryArray []LogEntry
							currNextIdx := rf.nextIndex[server]
                            logIdx := getActualIdx(rf.nextIndex[server], rf.indexOffset)

                            if logIdx < 0 {
								rf.mu.Unlock()
                                continue
                            }

                            for logIdx < len(rf.log) {
                                logEntryArray = append(logEntryArray, rf.log[logIdx])
                                logIdx += 1
                            }

                            pLogIdx := Min(rf.nextIndex[server]-1, getOffsetIdx(len(rf.log)-1, rf.indexOffset))
                            if pLogIdx < 0 {
                                pLogIdx = 0
                            }

                            var pLogTerm int
                            if getActualIdx(pLogIdx, rf.indexOffset) < 0 {
                                //_, _, snapshot := rf.readSnapshotPersist()
                                //pLogIdx  = snapshot.LastIncIdx
                                //pLogTerm = snapshot.LastIncTerm
                                pLogIdx  = rf.prevLogIdx
                                pLogTerm = rf.prevLogTerm
                            } else {
                                pLogTerm = rf.log[getActualIdx(pLogIdx, rf.indexOffset)].Term
                            }

							fmt.Println("Leader: srv", rf.me, "For server", server, "PrevLogIndex", pLogIdx, "rf.log", len(rf.log))
                            args := &AppendEntriesArgs {
                                LeadersTerm  : currentTerm                ,
                                LeaderId     : rf.me                      ,
                                PrevLogIndex : pLogIdx                    ,
                                PrevLogTerm  : pLogTerm				      ,
                                LogEntries   : logEntryArray              ,
                                LeaderCommit : rf.commitIndex             ,
                                IsEmpty      : len(logEntryArray)==0}
                            rf.mu.Unlock()
                            reply := &AppendEntriesReply{}
                            fmt.Println("Leader: srv", rf.me, "Sending AppendEntries to srv", server, "args", args)
                            ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
                            rf.mu.Lock()
                            fmt.Println("Leader: srv", rf.me, "Received reply from srv", server, "reply", reply)
                            if ok && currentTerm == rf.currentTerm {
                                if reply.Success {
                                    if currNextIdx + len(logEntryArray) > rf.nextIndex[server] {
                                        rf.nextIndex[server] = currNextIdx + len(logEntryArray)
                                    }
                                    if rf.nextIndex[server] > getOffsetIdx(len(rf.log), rf.indexOffset) {
                                        rf.nextIndex[server] = getOffsetIdx(len(rf.log), rf.indexOffset)
                                    }
                                    rf.matchIndex[server]   = rf.nextIndex[server] - 1
                                    AppendEntriesChannel <- reply
                                    fmt.Println("Leader: srv", rf.me, "Breaking off for srv", server)
                                    rf.mu.Unlock()
                                    break
                                } else if reply.Term > currentTerm {
                                    AppendEntriesChannel <- reply
                                    rf.mu.Unlock()
                                    fmt.Println("Leader: srv", rf.me, "Follower ", server, "has higher term. Breaking off")
                                    break
                                } else {
                                    rf.nextIndex[server]   = reply.ConflictEntryIdx
                                    fmt.Println("Leader: srv", rf.me, "Decrementing nextIdx for srv", server, "nextIdx", rf.nextIndex[server])
									if rf.nextIndex[server] < 1 {
										rf.nextIndex[server] = 1
									}
                                }
                            } else if ok && reply.Term > rf.currentTerm {
                                AppendEntriesChannel <- reply
                                rf.mu.Unlock()
                                break
                            } else {
                                AppendEntriesChannel <- nil
                                rf.mu.Unlock()
                                break
                            }
                            rf.mu.Unlock()
                        }
                        return
                    }(idx, rf.currentTerm)
    	        }
    		}
	        rf.mu.Unlock()

	        go func() {
    			numRepliesReceived := 0
    			numSuccessReceived := 1
	            updateToFollower := false
    	        rf.mu.Lock()
        	    highestTermSeen := rf.currentTerm
            	rf.mu.Unlock()
	            for reply := range AppendEntriesChannel {
    	            numRepliesReceived += 1
        	        if reply != nil {
        		        if reply.Success {
        	    	        numSuccessReceived += 1
    	            	} else if reply.Term > highestTermSeen {
                        	highestTermSeen = reply.Term
	    				    updateToFollower = true
	    			    }
    	            }
        	        if numRepliesReceived == len(rf.peers)-1 || numSuccessReceived > len(rf.peers) / 2 || updateToFollower {
            	        break
	                }
    	    	}

            	fmt.Println("Leader: Heartbeat reply on srv", rf.me, "numRepliesReceived", numRepliesReceived, "numSuccessReceived", numSuccessReceived, "updateToFollower", updateToFollower)
            	rf.mu.Lock()
	    		if updateToFollower && rf.currentTerm < highestTermSeen {
		    		rf.status = FOLLOWER
	                rf.currentTerm = highestTermSeen
                	rf.votedFor = -1
                    rf.persist()
            	    fmt.Println("Leader: srv", rf.me, "Found a srv with higer term")
    			} else if rf.status == LEADER {
					fmt.Println("Leader: srv", rf.me, "Current commitIndex:", rf.commitIndex)
/*
					N := rf.commitIndex
                    for {
                        N += 1
                        majority := 1
                        for idx := 0; idx < len(rf.peers); idx++ {
                            fmt.Println("Leader: srv", rf.me, "Follower:", idx, "MatchIndex:", rf.matchIndex[idx])
                            if idx != rf.me && rf.matchIndex[idx] >= N {
                                majority += 1
                            }
                        }
                        if !(majority > len(rf.peers)/2) {
                            N -= 1
                            break
                        } else if rf.log[getActualIdx(N, rf.indexOffset)].Term != rf.currentTerm {
                            N -= 1
                            break
                        }
                    }
*/
					N := getOffsetIdx(len(rf.log)-1, rf.indexOffset)
                    for N > rf.commitIndex {
                        majority := 1
                        for idx := 0; idx < len(rf.peers); idx++ {
                            fmt.Println("Leader: srv", rf.me, "Follower:", idx, "MatchIndex:", rf.matchIndex[idx])
                            if idx != rf.me && rf.matchIndex[idx] >= N {
                                majority += 1
                            }
                        }
                        if (majority > len(rf.peers)/2) && (rf.log[getActualIdx(N, rf.indexOffset)].Term == rf.currentTerm) {
                            break
                        }
						N -= 1
                    }

					fmt.Println("Leader: srv", rf.me, "Updating commitIndex to N:", N)
                    if N > rf.commitIndex {
                        rf.commitIndex = N
	                    go func() {
    	                    fmt.Println("Leader srv", rf.me, " ApplyMsgSender thread: ", "rf.log", len(rf.log), "commitIdx", rf.commitIndex, "lastApplied", rf.lastApplied)
        	                for {
            	                rf.nu.Lock()
                	            rf.mu.Lock()
                    	        if rf.lastApplied == rf.commitIndex {
                        	        rf.mu.Unlock()
                            	    rf.nu.Unlock()
                                	break
                            	}
                            	rf.lastApplied += 1
	                            //fmt.Println("Leader srv", rf.me, " ApplyMsgSender thread: ", "commitIdx", rf.commitIndex, "lastApplied", rf.lastApplied, "rf.log", len(rf.log))
    	                        appMsg := ApplyMsg {
        	                        Index       : rf.lastApplied,
            	                    Command     : rf.log[getActualIdx(rf.lastApplied, rf.indexOffset)].Log,
                                    Term        : rf.log[getActualIdx(rf.lastApplied, rf.indexOffset)].Term}

	                            //fmt.Println("Leader srv", rf.me, " ApplyMsgSender thread: Sending ApplyMsg", appMsg)
    	                        rf.mu.Unlock()
        	                    rf.applyChan <- appMsg
            	                rf.nu.Unlock()
                	        }
                    	    fmt.Println("Leader srv", rf.me, "ApplyMsgSender thread: Sent all ApplyMsg", "commitIdx", rf.commitIndex, "lastApplied", rf.lastApplied)
                    	}()
                    }
                }
	            rf.mu.Unlock()
	            return
	        }()
		}
        time.Sleep(2*time.Millisecond)
    }
    rf.mu.Lock()
    if rf.status == FOLLOWER {
        fmt.Println("Leader: srv", rf.me, "becoming a follower")
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
		if elapsed > rf.electionTimeout {
            rf.mu.Lock()
		    //Increment current term
		    rf.currentTerm += 1
		    rf.votedFor = -1

		    //Vote for self 
		    rf.votedFor = rf.me
            rf.persist()
		    //Reset election timer
		    rf.electionTimer = time.Now()

			var lastLogIdx int
			var lastLogTerm int
			if len(rf.log) > 0 {
				lastLogIdx  = getOffsetIdx(len(rf.log)-1, rf.indexOffset)
				lastLogTerm = rf.log[len(rf.log)-1].Term
			} else {
            	//_, _, snapshot := rf.readSnapshotPersist()
            	//lastLogIdx  = snapshot.LastIncIdx
            	//lastLogTerm = snapshot.LastIncTerm
                lastLogIdx  = rf.prevLogIdx
                lastLogTerm = rf.prevLogTerm
			}

		    args := &RequestVoteArgs{
		        CandidatesTerm : rf.currentTerm,
        		CandidateId    : rf.me     ,
    		    LastLogIndex   : lastLogIdx,
	        	LastLogTerm    : lastLogTerm}
		    RequestVoteChannel := make(chan *RequestVoteReply, len(rf.peers)-1)
		    //Send Request Vote RPCs to everyone but myself
		    for idx := 0; idx < len(rf.peers); idx++ {
                if idx != rf.me {
                    fmt.Println("Candidate srv ", rf.me, "Sending Request Vote", args, " to srv ", idx)
    		        go func (server int, currentTerm int) {
                            reply := &RequestVoteReply{}
	                    	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
                            rf.mu.Lock()
                            if (ok && rf.currentTerm == currentTerm) || (ok && reply.Term > rf.currentTerm) {
                                RequestVoteChannel <- reply
                            } else {
        	                	RequestVoteChannel <- nil
            	        	}
                            rf.mu.Unlock()
    	        	}(idx, rf.currentTerm)
                }
    		}

            rf.mu.Unlock()

            go func() {
                updateToFollower := false
                numRepliesReceived := 0
                numVotesReceived := 1
                rf.mu.Lock()
                highestTermSeen := rf.currentTerm
                rf.mu.Unlock()
                for reply := range RequestVoteChannel {
                    numRepliesReceived += 1
    		        if reply != nil {
    		            if reply.VoteGranted {
        		            numVotesReceived += 1
	    	            } else if reply.Term > highestTermSeen  {    //Update to follower with term received in reply
                            highestTermSeen = reply.Term
	                	    updateToFollower = true
	            	    }
	        	    }
    	        	if numRepliesReceived == len(rf.peers) - 1 || numVotesReceived > len(rf.peers) / 2 {
                        break
                    }
    	    	}
                fmt.Println("Candidate: election finished on srv ", rf.me, "numRepliesReceived", numRepliesReceived, "numVotesReceived", numVotesReceived, "updateToFollower", updateToFollower)
                rf.mu.Lock()
                quorum := len(rf.peers) / 2

        		if updateToFollower && rf.currentTerm < highestTermSeen {
                    rf.currentTerm = highestTermSeen
                    rf.votedFor = -1
    		    	rf.status = FOLLOWER
                    rf.persist()
                    fmt.Println("Candidate: srv", rf.me, "found a server with higher term")
        		} else if rf.status == CANDIDATE && numVotesReceived > quorum {
			        rf.status = LEADER
                    rf.leaderId = rf.me
                    fmt.Println("Candidate: srv", rf.me, "becoming leader")
		            go rf.ActAsLeader()
                }

                rf.mu.Unlock()
                return
            }()
		}

        time.Sleep(2*time.Millisecond)
        rf.mu.Lock()
		elapsed = time.Since(rf.electionTimer)
        rf.mu.Unlock()
	}
    rf.mu.Lock()
	if rf.status == FOLLOWER {
        fmt.Println("Candidate: srv", rf.me,"becoming follower")
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
    rf.applyChan = applyCh

    rf.mu.Lock()
    defer rf.mu.Unlock()
	// Your initialization code here (2A, 2B, 2C).
    ok := rf.readPersist(persister.ReadRaftState())
    if !ok {
        rf.currentTerm = 0
        rf.votedFor    = -1
        rf.indexOffset = -1
        rf.prevLogIdx   = -1
        rf.prevLogTerm  = -1
        var v interface{}
        lg := LogEntry{
            Term : 0,
            Log  : v}
        rf.log = append(rf.log, lg)
    }

    rf.nextIndex = make([]int, len(rf.peers))
    rf.matchIndex = make([]int, len(rf.peers))

    rf.commitIndex = rf.indexOffset
    rf.lastApplied = rf.indexOffset

	if rf.commitIndex == -1 && rf.lastApplied == -1 {
		rf.commitIndex = 0
		rf.lastApplied = 0
	}

    //s := rand.NewSource(time.Now().UnixNano())
    r1 := rand.New(rand.NewSource(time.Now().UnixNano()))
    numOut1 := int(5 + r1.Float64() * 5)

    newSeed := ((me + 1) * 10)
    //r2 := rand.New(rand.NewSource(newSeed))
    //numOut2 := int(r2.Float64() * 100)
    numOut2 := numOut1 * 100 + newSeed

    tout := time.Duration(numOut2)

    rf.electionTimeout = tout * time.Millisecond
    rf.heartbeatTimeout = 100 * time.Millisecond
	rf.heartbeatTimer   = time.Now()
    //rf.logId = time.Now().UnixNano()
    rf.status = FOLLOWER
    fmt.Println("Starting server", "me", rf.me, "log", rf.log, "electionTout", rf.electionTimeout, "heartbeatTout", rf.heartbeatTimeout, "status", rf.status)
    go rf.ActAsFollower()

	return rf
}
