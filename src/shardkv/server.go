package shardkv

import "shardmaster"
import (
    "encoding/gob"
    "labrpc"
//    "log"
    "raft"
    "sync"
    "strings"
    "fmt"
    "time"
    "bytes"
)

const (
        DOWN = iota
        UP
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
	ConfigChangeArgs	ConfigChange
}

type ShardKV struct {
	mu           sync.Mutex
	nu		     sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	masters      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
    clientReqMap    map[int64]int64
    applyIdx        int
    applyTerm       int
    status          int

    persister       *raft.Persister

    kvMap           map[string][]string

    mck             *shardmaster.Clerk
    config          shardmaster.Config
}

type PersistSnapshotData struct {
    LastIncIdx      int
    LastIncTerm     int
    KvMap           map[string][]string
    ClientReqMap    map[int64]int64
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	fmt.Println("Get on ", "gid: ", kv.gid, "srv: ", kv.me, "Starting args:", *args)
    // Check leader
    term, isLeader := kv.rf.GetState()
    if !isLeader {
        reply.WrongLeader = true
        reply.Err         = ErrWrongLeader
        fmt.Println("Get on ", "gid: ", kv.gid, "srv: ", kv.me, " Not a Leader")
        kv.mu.Unlock()
        return
    }
    //Check group
    shard := key2shard(args.Key)
    if kv.gid != kv.config.Shards[shard] {
		reply.Err			= ErrWrongGroup
		fmt.Println("Get on ", "gid: ", kv.gid, "srv: ", kv.me, " Wrong Group")
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
		fmt.Println("Get on ", "gid: ", kv.gid, "srv: ", kv.me, " Duplicate command ")
		kv.mu.Unlock()
		return
	}

	getOp := Op{OpId            : args.OpId,
				ClientId        : args.ClientId,
				Operation       : "Get",
				Key             : args.Key}

	index, term, isLeader := kv.rf.Start(getOp)
	fmt.Println("Get on ", "gid: ", kv.gid, "srv: ", kv.me, " Start Called, index: ", index, " Op:", getOp)

	if !isLeader {
		reply.WrongLeader = true
		reply.Err		  = ErrWrongLeader
		kv.mu.Unlock()
		return
	}

	applyIdxChan := make(chan bool)
	kv.mu.Unlock()

	go func () {
        for {
            kv.mu.Lock()
			fmt.Println("*********** Get Waiting on ", "gid: ", kv.gid, "srv: ", kv.me, " index: ", index, "applyIdx: ", kv.applyIdx, " Op:", getOp)
            newterm, isCurrLeader := kv.rf.GetState()
            idxApplied := (kv.applyIdx >= index)
            kv.mu.Unlock()
            if idxApplied {
        	    	appMsg, ok, logCutoff := kv.rf.GetLogAtIndex(index)
    	        	if ok {
	                	nxtOp := appMsg.Command.(Op)
                		if nxtOp.OpId == args.OpId && nxtOp.ClientId == args.ClientId {
                	    	applyIdxChan <- true
            	    	} else {
        	           		applyIdxChan <- false
    	            	}
		            	fmt.Println("Get on ", "gid: ", kv.gid, "srv: ", kv.me, "index", index, " Op:", nxtOp, "Breaking Off")
	                	break
	            	} else if logCutoff {
                        fmt.Println("Get on ", "gid: ", kv.gid, "srv: ", kv.me, "index", index, "Breaking Off. Not removed by snapshot")
                        applyIdxChan <- false
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

	fmt.Println("Get on ", "gid: ", kv.gid, "srv: ", kv.me, " args:", *args, " reply: ", *reply)
	kv.mu.Unlock()
	return
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	fmt.Println("PutAppend on ", "gid: ", kv.gid, "srv: ", kv.me, "Starting args:", *args)
    // Check leader
    term, isLeader := kv.rf.GetState()
    if !isLeader {
        reply.WrongLeader = true
        reply.Err         = ErrWrongLeader
        fmt.Println("PutAppend on ", "gid: ", kv.gid, "srv: ", kv.me, " Not a Leader")
        kv.mu.Unlock()
        return
    }
    //Check group
    shard := key2shard(args.Key)
    if kv.gid != kv.config.Shards[shard] {
        reply.Err           = ErrWrongGroup
        fmt.Println("PutAppend on ", "gid: ", kv.gid, "srv: ", kv.me, " Wrong Group")
        kv.mu.Unlock()
        return
    }

    //Duplicate Detection
    opId, keyFound := kv.clientReqMap[args.ClientId]
    if keyFound && opId == args.OpId {
		reply.WrongLeader = false
		reply.Err         = OK
        fmt.Println("PutAppend on ", "gid: ", kv.gid, "srv: ", kv.me, " Duplicate command ")
        kv.mu.Unlock()
        return
    }

    getOp := Op{OpId            : args.OpId,
                ClientId        : args.ClientId,
                Operation       : args.Op,
                Key             : args.Key,
				Value			: args.Value}

    index, term, isLeader := kv.rf.Start(getOp)
    fmt.Println("PutAppend on ", "gid: ", kv.gid, "srv: ", kv.me, " Start Called, index: ", index, " Op:", getOp)

    if !isLeader {
        reply.WrongLeader = true
        reply.Err         = ErrWrongLeader
		kv.mu.Unlock()
        return
    }

    applyIdxChan := make(chan bool)
	kv.mu.Unlock()

    go func () {
        for {
            kv.mu.Lock()
			fmt.Println("########### PutAppend Waiting on ", "gid: ", kv.gid, "srv: ", kv.me, " index: ", index, "applyIdx: ", kv.applyIdx, " Op:", getOp)
            newterm, isCurrLeader := kv.rf.GetState()
            idxApplied := (kv.applyIdx >= index)
            kv.mu.Unlock()
            if idxApplied {
            	    appMsg, ok, logCutoff := kv.rf.GetLogAtIndex(index)
        	        if ok {
    	                nxtOp := appMsg.Command.(Op)
	        	        if nxtOp.OpId == args.OpId && nxtOp.ClientId == args.ClientId {
	    	                applyIdxChan <- true
		                } else {
    						applyIdxChan <- false
	        	        }
	    	            fmt.Println("PutAppend on ", "gid: ", kv.gid, "srv: ", kv.me, "index", index, " Op:", nxtOp, "Breaking Off")
    		            break
	                } else if logCutoff {
                        fmt.Println("PutAppend on ", "gid: ", kv.gid, "srv: ", kv.me, "index", index,  "Breaking Off. Log cutoff by snapshot")
                        applyIdxChan <- false
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

	kv.mu.Lock()
    if msgApplied {
		reply.WrongLeader = false
		reply.Err         = OK
    } else {
        reply.WrongLeader = false
        reply.Err         = ErrNoConcensus
    }

    fmt.Println("Get on ", "gid: ", kv.gid, "srv: ", kv.me, " args:", *args, " reply: ", *reply)
    kv.mu.Unlock()
    return
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
    kv.mu.Lock()
    fmt.Println("Killing kv server ", "gid: ", kv.gid, "srv: ", kv.me, " gid:", kv.gid)
    kv.rf.Kill()
    kv.status = DOWN
    kv.mu.Unlock()
}

func (kv *ShardKV) snapshotPersist() {
    w := new(bytes.Buffer)
    e := gob.NewEncoder(w)

    var pData PersistSnapshotData
    pData = PersistSnapshotData{kv.applyIdx, kv.applyTerm, kv.kvMap, kv.clientReqMap}
    e.Encode(pData)
    fmt.Println("Snapshot Persist: srv", "gid: ", kv.gid, "srv: ", kv.me, " pData.ApplyIdx:", pData.LastIncIdx, " pData.ApplyTerm:", pData.LastIncTerm)
    data := w.Bytes()
    kv.persister.SaveSnapshot(data)
}

func (kv *ShardKV) readSnapshotPersist(data []byte) bool {
    var pData PersistSnapshotData
    r := bytes.NewBuffer(data)
    d := gob.NewDecoder(r)
    d.Decode(&pData)
    if data == nil || len(data) < 1 { // bootstrap without any state?
        return false
    }
    fmt.Println("Read Snapshot Persist: srv", "gid: ", kv.gid, "srv: ", kv.me, " pData.ApplyIdx:", pData.LastIncIdx, " pData.ApplyTerm:", pData.LastIncTerm)
    kv.applyIdx     = pData.LastIncIdx
    kv.applyTerm    = pData.LastIncTerm

    kv.kvMap        = pData.KvMap
    kv.clientReqMap = pData.ClientReqMap
    return true
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots with
// persister.SaveSnapshot(), and Raft should save its state (including
// log) with persister.SaveRaftState().
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardmaster.
//
// pass masters[] to shardmaster.MakeClerk() so you can send
// RPCs to the shardmaster.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use masters[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.masters = masters

	// Your initialization code here.
    kv.persister = persister
    kv.kvMap = make(map[string][]string)
	kv.clientReqMap = make(map[int64]int64)
    kv.applyCh = make(chan raft.ApplyMsg)

	// Use something like this to talk to the shardmaster:
	kv.mck = shardmaster.MakeClerk(kv.masters)

	kv.mu.Lock()
	defer kv.mu.Unlock()
    ok := kv.readSnapshotPersist(persister.ReadSnapshot())
    if !ok {
		kv.applyIdx  = -1
		kv.applyTerm = -1
	}
    kv.status = UP
    kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.rf.UpdateRaftState(kv.applyIdx, kv.applyTerm)
	kv.config = kv.mck.Query(-1)
	fmt.Println("On kvserver:", "gid: ", kv.gid, "srv: ", kv.me, "Updating config to:", kv.config)

	go func() {
		fmt.Println("ApplyCh on srv", "gid: ", kv.gid, "srv: ", kv.me, " Starting to listen")
        for {
            kv.mu.Lock()
            if kv.status != UP {
                fmt.Println("Ending kvserver:", "gid: ", kv.gid, "srv: ", kv.me, "listening thread")
                kv.mu.Unlock()
                break
            }
            kv.mu.Unlock()
			appMsg := <-kv.applyCh
			kv.mu.Lock()
			if appMsg.UseSnapshot {
                fmt.Println("Got new snapshot on srv", "gid: ", kv.gid, "srv: ", kv.me, "Applying")
				kv.readSnapshotPersist(appMsg.Snapshot)
				kv.snapshotPersist()
			} else {
            	kv.applyIdx = appMsg.Index
				kv.applyTerm = appMsg.Term

				nxtOp := appMsg.Command.(Op)
				fmt.Println("ApplyCh on srv", "gid: ", kv.gid, "srv: ", kv.me, "nxtOp", nxtOp)
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
			}
			kv.mu.Unlock()
        }
	}()

    go func(){
		if maxraftstate != -1 {
	        for {
				kv.mu.Lock()

	            if kv.status != UP {
    	            fmt.Println("Ending kvserver:", "gid: ", kv.gid, "srv: ", kv.me, "snapshot thread. ending")
        	        kv.mu.Unlock()
            	    break
            	}

                ratio := float64(persister.RaftStateSize()) / float64(maxraftstate)
	            if ratio > 0.95 {
                    fmt.Println("Snapshot thread: srv:", "gid: ", kv.gid, "srv: ", kv.me, "raftStateSize:", persister.RaftStateSize(), "maxraftstate", maxraftstate, "ratio", ratio)
                    fmt.Println("Snapshot thread: srv:", "gid: ", kv.gid, "srv: ", kv.me, "applyIdx:", kv.applyIdx, "applyTerm:", kv.applyTerm)
                    kv.snapshotPersist()
                    kv.rf.DiscardOldLogs(kv.applyIdx, kv.applyTerm)
				}
				kv.mu.Unlock()
                time.Sleep(time.Millisecond)
	        }
		}
    }()

    go func() {
        for {
            kv.mu.Lock()
            if kv.status != UP {
                fmt.Println("Updating config thread: kvserver:", "gid: ", kv.gid, "srv: ", kv.me, ".....Ending.....")
                kv.mu.Unlock()
                break
            }
			_, isLeader := kv.rf.GetState()
    		if !isLeader {
				kv.mu.Unlock()
				continue
			}
            kv.mu.Unlock()

            newconfig := kv.mck.Query(-1)
			UpdateWithNewConfiguration(newconfig)

            //if newconfig.Num > kv.config.Num {
		    fmt.Println("Updating config thread: kvserver:", "gid: ", kv.gid, "srv: ", kv.me, "New config: ", newconfig)
            //}
            time.Sleep(50*time.Millisecond)
        }
    }()

	return kv
}

func (kv *ShardKV) UpdateWithNewConfiguration(newConfig shardmaster.Config) {
	kv.mu.Lock()
	if kv.config.Num >= newConfig.Num {
		kv.mu.Unlock()
		return
	}

	missingGidShardMap := make(map[int][]int)
	s_idx := 0
	for s_idx < shardmaster.NShards {
		oldgid := kv.config.Shards[s_idx]
		newgid := newConfig.Shards[s_idx]
		if (newgid == kv.gid) && (oldgid != kv.gid) && oldgid != 0 {
			_, ok := missingGidShardMap[oldgid]
			if !ok {
				missingGidShardMap[oldgid] = []int{i}
			} else {
				missingGidShardMap[oldgid] = append(missingGidShardMap[oldgid], i)
			}
		}
		s_idx += 1
	}

	kv.mu.Unlock()

	var configChange ConfigChange
	configChange.ClientReqMap = make(map[int64]int64)
	configChange.NewKVs		  = make(map[string][string])
	configChange.Config	      = shardmaster.Config{Config	:	newConfig.Num}
	//configChange.Config.Num   = newConfig.Num
	configChange.Config.Shards   = make([]int, shardmaster.NShards)
	configChange.Config.Groups   = make(map[int][]string)

	for i, value := newConfig.Shards {
		configChange.Config.Shards[i] = value
	}
	for key, val := newConfig.Groups {
		configChange.Config.Groups[key] = val
	}

	var wait sync.WaitGroup
	allClear := true

	for gid, shards := range missingGidShardMap {
		wait.Add(1)
		go func(gid int, shards []int) {
			defer wait.Done()
			getShardsArgs := GetShardsArgs{
				ConfigNumber: newConfig.Num,
				RequestedShards: shards}

			var reply GetShardsReply

			rpcSuccess := false
			for _, server := range kv.config.Groups[gid] {
				srv := kv.make_end(server)
				ok := srv.Call("ShardKV.GetShards", &args, &reply)
				if ok {
					if reply.Err == OK {
						rpcSuccess = true
						break
					} else if reply.Err == ErrOldConfig {
						break
					}
				}
			}

			if rpcSuccess {
				rf.nu.Lock()
					for key, value := range reply.RequestedKVs {
						configChange.NewKVs[key] = value
					}
					for clientId, _ := range reply.ClientReqMap {
						_, there := configChange.ClientReqMap[clientId]
						if !there || (configChange.ClientReqMap[clientId] < reply.ClientReqMap[clientId]) {
							configChange.ClientReqMap[clientId] = reply.ClientReqMap[clientId]
						}
					}
				rf.nu.Unlock()
			} else {
				allClear = false
			}
		}(gid, shards)
	}
	wait.Wait()


	if allClear {
		kv.mu.Lock()
		fmt.Println("Config change consensus on ", "gid: ", kv.gid, "srv: ", kv.me, "Starting args:", configChange)
	    // Check leader
    	term, isLeader := kv.rf.GetState()
	    if !isLeader {
        	fmt.Println("Config change consensus on ", "gid: ", kv.gid, "srv: ", kv.me, " Not a Leader")
        	kv.mu.Unlock()
       	 	return
    	}

		getOp := Op{Operation       	: "Consensus",
					ConfigChangeArgs    : configChange}

		index, term, isLeader := kv.rf.Start(getOp)
		fmt.Println("Config change consensus on ", "gid: ", kv.gid, "srv: ", kv.me, " Start Called, index: ", index, " Op:", getOp)

		if !isLeader {
			kv.mu.Unlock()
			return
		}

		applyIdxChan := make(chan bool)
		kv.mu.Unlock()

		go func () {
    	    for {
            	kv.mu.Lock()
				fmt.Println("*********** Config change consensus Waiting on ", "gid: ", kv.gid, "srv: ", kv.me, " index: ", index, "applyIdx: ", kv.applyIdx, " Op:", getOp)
	            newterm, isCurrLeader := kv.rf.GetState()
    	        idxApplied := (kv.applyIdx >= index)
        	    kv.mu.Unlock()
            	if idxApplied {
        	    	appMsg, ok, logCutoff := kv.rf.GetLogAtIndex(index)
    	        	if ok {
	                	nxtOp := appMsg.Command.(Op)
                		if nxtOp.OpId == args.OpId && nxtOp.ClientId == args.ClientId {
                	    	applyIdxChan <- true
            	    	} else {
        	           		applyIdxChan <- false
    	            	}
		            	fmt.Println("Get on ", "gid: ", kv.gid, "srv: ", kv.me, "index", index, " Op:", nxtOp, "Breaking Off")
	                	break
	            	} else if logCutoff {
                        fmt.Println("Get on ", "gid: ", kv.gid, "srv: ", kv.me, "index", index, "Breaking Off. Not removed by snapshot")
                        applyIdxChan <- false
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

	fmt.Println("Get on ", "gid: ", kv.gid, "srv: ", kv.me, " args:", *args, " reply: ", *reply)
	kv.mu.Unlock()
	return
	}

}

//RPC Handler
func (kv *ShardKV) GetShards(args *GetShardsArgs, reply *GetShardsReply) {
    kv.mu.Lock()
	defer kv.mu.Unlock()

    _, isLeader := kv.rf.GetState()
    if !isLeader {
		reply.WrongLeader = true
		reply.Err = ErrWrongLeader
        return
    }

    if kv.config.Num < args.ConfigNumber {
        reply.Err = ErrOldConfig
        return
    }

	reply.WrongLeader = false
	reply.Err = OK

	reply.ClientReqMap = make(map[int64]int64)
	for key, value := range kv.clientReqMap {
		reply.ClientReqMap[key] = value
	}

	reply.RequestedKVs = make(map[string][]string)
	for key, value := range kv.kvMap {
		_, ok := args.RequestedShards[key2shard(key)]
		if ok {
			reply.RequestedKVs[key] = value
		}
	}
	return
}
