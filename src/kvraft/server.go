package raftkv

import (
	"encoding/gob"
	"labrpc"
	"log"
	"raft"
	"sync"
	"strings"
	"fmt"
    "time"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

const (
        FOLLOWER = iota
        CANDIDATE
        LEADER
      )

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
    OpId            int64
    ClientId        int64
    Operation       string
    Key             string
    Value           string
}

type RaftKV struct {
	mu			    sync.Mutex
	me			    int
	rf			    *raft.Raft
	applyCh		    chan raft.ApplyMsg
//	leaderCh	    chan raft.ApplyMsg
	clientReqMap	map[int64]int64
    applyIdx        int

	maxraftstate    int // snapshot if log grows this big

    kvMap		    map[string][]string
}

func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
	//fmt.Println("*********** kv Lock. srv:", kv.me, "Get1")
	kv.mu.Lock()
	fmt.Println("Get on ", kv.me, "Starting args:", *args)
	// Check leader
	term, isLeader := kv.rf.GetState()
    if !isLeader {
		reply.WrongLeader = true
		reply.Err		  = ErrWrongLeader
		fmt.Println("Get on ", kv.me, " Not a Leader")
		//fmt.Println("*********** kv Unlock. srv:", kv.me, "Get1")
		kv.mu.Unlock()
		return
    }

	//Duplicate Detection
    opId, keyFound := kv.clientReqMap[args.ClientId]
	if keyFound && opId == args.OpId {
		value, keyFound := kv.kvMap[args.Key]
        if keyFound {
            reply.WrongLeader = false
            reply.Err         = OK
            reply.Value       = strings.Join(value, "")
        } else {
            reply.WrongLeader = false
            reply.Err         = ErrNoKey
        }
		fmt.Println("Get on ", kv.me, " Duplicate command ")
		//fmt.Println("*********** kv Unlock. srv:", kv.me, "Get2")
		kv.mu.Unlock()
		return
	}

	getOp := Op{OpId            : args.OpId,
				ClientId        : args.ClientId,
				Operation       : "Get",
				Key             : args.Key}

	index, term, isLeader := kv.rf.Start(getOp)
	fmt.Println("Get on ", kv.me, " Start Called, index: ", index, " Op:", getOp)

	if !isLeader {
		reply.WrongLeader = true
		reply.Err		  = ErrWrongLeader
		//fmt.Println("*********** kv Unlock. srv:", kv.me, "Get3")
		kv.mu.Unlock()
		return
	}

	applyIdxChan := make(chan bool)
	//fmt.Println("*********** kv Unlock. srv:", kv.me, "Get4")
	kv.mu.Unlock()

	go func () {
        for {
			//fmt.Println("*********** kv Lock. srv:", kv.me, "GetWaitLoop")
            kv.mu.Lock()
			fmt.Println("*********** Get Wating on ", kv.me, " index: ", index, "applyIdx: ", kv.applyIdx, " Op:", getOp)
            newterm, isCurrLeader := kv.rf.GetState()
            idxApplied := (kv.applyIdx >= index)
			//fmt.Println("*********** kv Unlock. srv:", kv.me, "GetWaitLoop")
            kv.mu.Unlock()
            if idxApplied {
        	    appMsg, ok := kv.rf.GetLogAtIndex(index)
    	        if ok {
	                nxtOp := appMsg.Command.(Op)
                	if nxtOp.OpId == args.OpId && nxtOp.ClientId == args.ClientId {
                	    applyIdxChan <- true
            	    } else {
        	            applyIdxChan <- false
    	            }
		            fmt.Println("Get on ", kv.me, "index", index, " Op:", nxtOp, "Breaking Off")
	                break
	            }
			} else if (newterm != term || !isCurrLeader) {
                applyIdxChan <- false
                break
            }
            time.Sleep(time.Millisecond)
        }
	}()

	msgApplied := <-applyIdxChan

	//fmt.Println("*********** kv Lock. srv:", kv.me, "Get2")
	kv.mu.Lock()
	if msgApplied {
		value, keyFound := kv.kvMap[args.Key]
		if keyFound {
			reply.WrongLeader = false
			reply.Err		  = OK
			reply.Value		  = strings.Join(value, "")
		} else{
            reply.WrongLeader = false
            reply.Err         = ErrNoKey
		}
	} else {
		reply.WrongLeader = false
		reply.Err		  = ErrNoConcensus
	}

	fmt.Println("Get on ", kv.me, " args:", *args, " reply: ", *reply)
	//fmt.Println("*********** kv Unlock. srv:", kv.me, "Get5")
	kv.mu.Unlock()
	return
}

func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	//fmt.Println("########### kv Lock. srv:", kv.me, "Put1")
	kv.mu.Lock()
	fmt.Println("PutAppend on ", kv.me, "Starting args:", *args)
    // Check leader
	term, isLeader := kv.rf.GetState()
    if !isLeader {
        reply.WrongLeader = true
        reply.Err         = ErrWrongLeader
		fmt.Println("PutAppend on ", kv.me, " Not a Leader")
		//fmt.Println("########### kv Unlock. srv:", kv.me, "Put1")
		kv.mu.Unlock()
        return
    }

    //Duplicate Detection
    opId, keyFound := kv.clientReqMap[args.ClientId]
    if keyFound && opId == args.OpId {
		reply.WrongLeader = false
		reply.Err         = OK
        fmt.Println("PutAppend on ", kv.me, " Duplicate command ")
		//fmt.Println("########### kv Unlock. srv:", kv.me, "Put2")
        kv.mu.Unlock()
        return
    }

    getOp := Op{OpId            : args.OpId,
                ClientId        : args.ClientId,
                Operation       : args.Op,
                Key             : args.Key,
				Value			: args.Value}

    index, term, isLeader := kv.rf.Start(getOp)
    fmt.Println("PutAppend on ", kv.me, " Start Called, index: ", index, " Op:", getOp)

    if !isLeader {
        reply.WrongLeader = true
        reply.Err         = ErrWrongLeader
		kv.mu.Unlock()
        return
    }

    applyIdxChan := make(chan bool)
	//fmt.Println("########### kv Unlock. srv:", kv.me, "Put3")
	kv.mu.Unlock()

    go func () {
        for {
			//fmt.Println("########### kv Lock. srv:", kv.me, "PutAppendWaitLoop")
            kv.mu.Lock()
			fmt.Println("########### PutAppend Waiting on ", kv.me, " index: ", index, "applyIdx: ", kv.applyIdx, " Op:", getOp)
            newterm, isCurrLeader := kv.rf.GetState()
            idxApplied := (kv.applyIdx >= index)
			//fmt.Println("########### kv Unlock. srv:", kv.me, "PutAppendWaitLoop")
            kv.mu.Unlock()
            if idxApplied {
                appMsg, ok := kv.rf.GetLogAtIndex(index)
                if ok {
                    nxtOp := appMsg.Command.(Op)
        	        if nxtOp.OpId == args.OpId && nxtOp.ClientId == args.ClientId {
    	                applyIdxChan <- true
	                } else {
    					applyIdxChan <- false
        	        }
	                fmt.Println("PutAppend on ", kv.me, "index", index, " Op:", nxtOp, "Breaking Off")
    	            break
                }
            } else if (newterm != term || !isCurrLeader) {
                applyIdxChan <- false
                break
            }
            time.Sleep(time.Millisecond)
		}
    }()

    msgApplied := <-applyIdxChan

	//fmt.Println("########### kv Lock. srv:", kv.me, "Put2")
	kv.mu.Lock()
    if msgApplied {
		reply.WrongLeader = false
		reply.Err         = OK
    } else {
        reply.WrongLeader = false
        reply.Err         = ErrNoConcensus
    }

    fmt.Println("Get on ", kv.me, " args:", *args, " reply: ", *reply)
	//fmt.Println("########### kv Unlock. srv:", kv.me, "Put4")
    kv.mu.Unlock()
    return
}

// the tester calls Kill() when a RaftKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *RaftKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots with persister.SaveSnapshot(),
// and Raft should save its state (including log) with persister.SaveRaftState().
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *RaftKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(RaftKV)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
    kv.kvMap = make(map[string][]string)
	kv.clientReqMap = make(map[int64]int64)
	kv.applyCh = make(chan raft.ApplyMsg)
    kv.applyIdx = 0
	//kv.leaderCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	// You may need initialization code here.

	go func() {
		fmt.Println("ApplyCh on srv", kv.me, " Starting to listen")
        for {
			appMsg := <-kv.applyCh

			//fmt.Println("%%%%%%%%%%% kv Lock. srv:", kv.me, "Apply")
			kv.mu.Lock()
            //fmt.Println("%%%%%%%%%%% kv Lock. srv:", kv.me, "Apply Lock Taken")

            kv.applyIdx = appMsg.Index
			nxtOp := appMsg.Command.(Op)
			fmt.Println("ApplyCh on srv", kv.me, "nxtOp", nxtOp)
            //Duplicate Detection
            opId, keyFound := kv.clientReqMap[nxtOp.ClientId]
            if !(keyFound && opId == nxtOp.OpId) {
    			if nxtOp.Operation == "Put" {
	    			kv.kvMap[nxtOp.Key] = []string{nxtOp.Value}
		    	} else if nxtOp.Operation == "Append" {
                    value, keyFound := kv.kvMap[nxtOp.Key]
                    if keyFound {
                        kv.kvMap[nxtOp.Key] = append(value, nxtOp.Value)
                    } else {
                        kv.kvMap[nxtOp.Key] = []string{nxtOp.Value}
                    }
                }
			    kv.clientReqMap[nxtOp.ClientId] = nxtOp.OpId
            }
			//fmt.Println("%%%%%%%%%%% kv Unlock. srv:", kv.me, "Apply")
			kv.mu.Unlock()

//			_, isLeader := kv.rf.GetState()
//			if isLeader {
//				kv.leaderCh <- appMsg
//			}
        }
	}()
	return kv
}

/*
        for {
            appMsg := <-kv.leaderCh
            if appMsg.Index == index {
                nxtOp := appMsg.Command.(Op)
                if nxtOp.OpId == args.OpId && nxtOp.ClientId == args.ClientId {
                    applyIdxChan <- true
                } else {
                    applyIdxChan <- false
                    //Not needed
                    //kv.leaderCh <- appMsg
                }
                fmt.Println("Get on ", kv.me, "index:", index, " Op:", nxtOp, "Breaking Off")
                break
            } else {
                kv.leaderCh <- appMsg
                fmt.Println("Get on ", kv.me, "index:", index, " AppMsg: ", appMsg, "Putting Back")
            }
        }
*/
/*
        for {
            appMsg := <-kv.leaderCh
            if appMsg.Index == index {
                nxtOp := appMsg.Command.(Op)
                if nxtOp.OpId == args.OpId && nxtOp.ClientId == args.ClientId {
                    applyIdxChan <- true
                } else {
                    applyIdxChan <- false
                    //Not needed
                    //kv.leaderCh <- appMsg
                }
                fmt.Println("Get on ", kv.me, "index:", index, " Op:", nxtOp, "Breaking Off")
                break
            } else {
                kv.leaderCh <- appMsg
                fmt.Println("Get on ", kv.me, "index:", index, " AppMsg: ", appMsg, "Putting Back")
            }
        }
*/
