package shardmaster


import (
	"raft"
 	"labrpc"
 	"sync"
 	"encoding/gob"
	"fmt"
    "time"
	"sort"
)

const (
        DOWN = iota
        UP
      )


type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	configs []Config // indexed by config num

	// Your data here.
    clientReqMap    map[int64]int64
    applyIdx        int
    applyTerm       int
    status          int
}


type Op struct {
	// Your data here.
    OpId            int64
    ClientId        int64
    Operation       string
	Servers		    map[int][]string
	GIDs			[]int
	Shard			int
	GID				int
	Num				int
//    Key             string
//    Value           string
}


func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
    sm.mu.Lock()
    fmt.Println("Join on ", sm.me, "Join args:", *args)
    // Check leader
    term, isLeader := sm.rf.GetState()
    if !isLeader {
        reply.WrongLeader = true
        reply.Err         = ErrWrongLeader
        fmt.Println("Join on ", sm.me, " Not a Leader")
        sm.mu.Unlock()
        return
    }

    //Duplicate Detection
    opId, keyFound := sm.clientReqMap[args.ClientId]
    if keyFound && opId == args.OpId {
		reply.WrongLeader = false
		reply.Err		  = OK
        fmt.Println("Join on ", sm.me, " Duplicate command ")
        sm.mu.Unlock()
        return
    }

    joinOp := Op{OpId            : args.OpId,
                 ClientId        : args.ClientId,
                 Operation       : "Join",
                 Servers         : args.Servers}

    index, term, isLeader := sm.rf.Start(joinOp)
    fmt.Println("Join on ", sm.me, " Start Called, index: ", index, " Op:", joinOp)

    if !isLeader {
        reply.WrongLeader = true
        reply.Err         = ErrWrongLeader
        sm.mu.Unlock()
        return
    }

    applyIdxChan := make(chan bool)
    sm.mu.Unlock()

    go func () {
        for {
            sm.mu.Lock()
            fmt.Println("*********** Join Waiting on ", sm.me, " index: ", index, "applyIdx: ", sm.applyIdx, " Op:", joinOp)
            newterm, isCurrLeader := sm.rf.GetState()
            idxApplied := (sm.applyIdx >= index)
            sm.mu.Unlock()
            if idxApplied {
                    appMsg, ok, logCutoff := sm.rf.GetLogAtIndex(index)
                    if ok {
                        nxtOp := appMsg.Command.(Op)
                        if nxtOp.OpId == args.OpId && nxtOp.ClientId == args.ClientId {
                            applyIdxChan <- true
                        } else {
                            applyIdxChan <- false
                        }
                        fmt.Println("Join on ", sm.me, "index", index, " Op:", nxtOp, "Breaking Off")
                        break
                    } else if logCutoff {
                        fmt.Println("Join on ", sm.me, "index", index, "Breaking Off. Not removed by snapshot")
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

    sm.mu.Lock()
    if msgApplied {
		reply.WrongLeader = false
		reply.Err         = OK
	} else {
        reply.WrongLeader = false
        reply.Err         = ErrNoConcensus
    }

    fmt.Println("Join on ", sm.me, " args:", *args, " reply: ", *reply)
    sm.mu.Unlock()
    return
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
    sm.mu.Lock()
    fmt.Println("Leave on ", sm.me, "Leave args:", *args)
    // Check leader
    term, isLeader := sm.rf.GetState()
    if !isLeader {
        reply.WrongLeader = true
        reply.Err         = ErrWrongLeader
        fmt.Println("Leave on ", sm.me, " Not a Leader")
        sm.mu.Unlock()
        return
    }

    //Duplicate Detection
    opId, keyFound := sm.clientReqMap[args.ClientId]
    if keyFound && opId == args.OpId {
		reply.WrongLeader = false
		reply.Err		  = OK
        fmt.Println("Leave on ", sm.me, " Duplicate command ")
        sm.mu.Unlock()
        return
    }

    leaveOp := Op{OpId            : args.OpId,
                  ClientId        : args.ClientId,
                  Operation       : "Leave",
                  GIDs            : args.GIDs}

    index, term, isLeader := sm.rf.Start(leaveOp)
    fmt.Println("Leave on ", sm.me, " Start Called, index: ", index, " Op:", leaveOp)

    if !isLeader {
        reply.WrongLeader = true
        reply.Err         = ErrWrongLeader
        sm.mu.Unlock()
        return
    }

    applyIdxChan := make(chan bool)
    sm.mu.Unlock()

    go func () {
        for {
            sm.mu.Lock()
            fmt.Println("*********** Leave Waiting on ", sm.me, " index: ", index, "applyIdx: ", sm.applyIdx, " Op:", leaveOp)
            newterm, isCurrLeader := sm.rf.GetState()
            idxApplied := (sm.applyIdx >= index)
            sm.mu.Unlock()
            if idxApplied {
                    appMsg, ok, logCutoff := sm.rf.GetLogAtIndex(index)
                    if ok {
                        nxtOp := appMsg.Command.(Op)
                        if nxtOp.OpId == args.OpId && nxtOp.ClientId == args.ClientId {
                            applyIdxChan <- true
                        } else {
                            applyIdxChan <- false
                        }
                        fmt.Println("Leave on ", sm.me, "index", index, " Op:", nxtOp, "Breaking Off")
                        break
                    } else if logCutoff {
                        fmt.Println("Leave on ", sm.me, "index", index, "Breaking Off. Not removed by snapshot")
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

    sm.mu.Lock()
    if msgApplied {
		reply.WrongLeader = false
		reply.Err         = OK
	} else {
        reply.WrongLeader = false
        reply.Err         = ErrNoConcensus
    }

    fmt.Println("Leave on ", sm.me, " args:", *args, " reply: ", *reply)
    sm.mu.Unlock()
    return
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
    sm.mu.Lock()
    fmt.Println("Move on ", sm.me, "Move args:", *args)
    // Check leader
    term, isLeader := sm.rf.GetState()
    if !isLeader {
        reply.WrongLeader = true
        reply.Err         = ErrWrongLeader
        fmt.Println("Move on ", sm.me, " Not a Leader")
        sm.mu.Unlock()
        return
    }

    //Duplicate Detection
    opId, keyFound := sm.clientReqMap[args.ClientId]
    if keyFound && opId == args.OpId {
		reply.WrongLeader = false
		reply.Err		  = OK
        fmt.Println("Move on ", sm.me, " Duplicate command ")
        sm.mu.Unlock()
        return
    }

    moveOp := Op{OpId            : args.OpId,
                 ClientId        : args.ClientId,
                 Operation       : "Move",
                 GID             : args.GID,
				 Shard			 : args.Shard}

    index, term, isLeader := sm.rf.Start(moveOp)
    fmt.Println("Move on ", sm.me, " Start Called, index: ", index, " Op:", moveOp)

    if !isLeader {
        reply.WrongLeader = true
        reply.Err         = ErrWrongLeader
        sm.mu.Unlock()
        return
    }

    applyIdxChan := make(chan bool)
    sm.mu.Unlock()

    go func () {
        for {
            sm.mu.Lock()
            fmt.Println("*********** Move Waiting on ", sm.me, " index: ", index, "applyIdx: ", sm.applyIdx, " Op:", moveOp)
            newterm, isCurrLeader := sm.rf.GetState()
            idxApplied := (sm.applyIdx >= index)
            sm.mu.Unlock()
            if idxApplied {
                    appMsg, ok, logCutoff := sm.rf.GetLogAtIndex(index)
                    if ok {
                        nxtOp := appMsg.Command.(Op)
                        if nxtOp.OpId == args.OpId && nxtOp.ClientId == args.ClientId {
                            applyIdxChan <- true
                        } else {
                            applyIdxChan <- false
                        }
                        fmt.Println("Move on ", sm.me, "index", index, " Op:", nxtOp, "Breaking Off")
                        break
                    } else if logCutoff {
                        fmt.Println("Move on ", sm.me, "index", index, "Breaking Off. Not removed by snapshot")
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

    sm.mu.Lock()
    if msgApplied {
		reply.WrongLeader = false
		reply.Err         = OK
	} else {
        reply.WrongLeader = false
        reply.Err         = ErrNoConcensus
    }

    fmt.Println("Move on ", sm.me, " args:", *args, " reply: ", *reply)
    sm.mu.Unlock()
    return
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
    sm.mu.Lock()
    fmt.Println("Query on ", sm.me, "Query args:", *args)
    // Check leader
    term, isLeader := sm.rf.GetState()
    if !isLeader {
        reply.WrongLeader = true
        reply.Err         = ErrWrongLeader
        fmt.Println("Query on ", sm.me, " Not a Leader")
        sm.mu.Unlock()
        return
    }

    //Duplicate Detection
    opId, keyFound := sm.clientReqMap[args.ClientId]
    if keyFound && opId == args.OpId {
		reply.WrongLeader = false
		reply.Err		  = OK
        fmt.Println("Query on ", sm.me, " Duplicate command ")
        sm.mu.Unlock()
        return
    }

    queryOp := Op{OpId            : args.OpId,
                  ClientId        : args.ClientId,
                  Operation       : "Query",
                  Num             : args.Num}

    index, term, isLeader := sm.rf.Start(queryOp)
    fmt.Println("Query on ", sm.me, " Start Called, index: ", index, " Op:", queryOp)

    if !isLeader {
        reply.WrongLeader = true
        reply.Err         = ErrWrongLeader
        sm.mu.Unlock()
        return
    }

    applyIdxChan := make(chan bool)
    sm.mu.Unlock()

    go func () {
        for {
            sm.mu.Lock()
            fmt.Println("*********** Query Waiting on ", sm.me, " index: ", index, "applyIdx: ", sm.applyIdx, " Op:", queryOp)
            newterm, isCurrLeader := sm.rf.GetState()
            idxApplied := (sm.applyIdx >= index)
            sm.mu.Unlock()
            if idxApplied {
                    appMsg, ok, logCutoff := sm.rf.GetLogAtIndex(index)
                    if ok {
                        nxtOp := appMsg.Command.(Op)
                        if nxtOp.OpId == args.OpId && nxtOp.ClientId == args.ClientId {
                            applyIdxChan <- true
                        } else {
                            applyIdxChan <- false
                        }
                        fmt.Println("Query on ", sm.me, "index", index, " Op:", nxtOp, "Breaking Off")
                        break
                    } else if logCutoff {
                        fmt.Println("Query on ", sm.me, "index", index, "Breaking Off. Not removed by snapshot")
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

    sm.mu.Lock()
    if msgApplied {
		reply.WrongLeader = false
		reply.Err         = OK
		if (args.Num < 0) || (args.Num >= len(sm.configs)) {
			reply.Config	= sm.configs[len(sm.configs)-1]
		} else {
			reply.Config    = sm.configs[args.Num]
		}
	} else {
        reply.WrongLeader = false
        reply.Err         = ErrNoConcensus
    }

    fmt.Println("Query on ", sm.me, " args:", *args, " reply: ", *reply)
    sm.mu.Unlock()
    return
}


//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
    sm.mu.Lock()
    sm.rf.Kill()
    sm.status = DOWN
    sm.mu.Unlock()
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	gob.Register(Op{})

	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}
	sm.configs[0].Num    = 0

	sm.applyCh = make(chan raft.ApplyMsg)

	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	sm.status = UP
    sm.applyIdx  = -1
    sm.applyTerm = -1
	sm.clientReqMap = make(map[int64]int64)

	// Your code here.
    go func() {
        fmt.Println("ApplyCh on ShardMaster", sm.me, " Starting to listen")
        for {
            sm.mu.Lock()
            if sm.status != UP {
                fmt.Println("Ending ShardMaster:", sm.me, "listening thread")
                sm.mu.Unlock()
                break
            }
            sm.mu.Unlock()

            appMsg := <-sm.applyCh

            sm.mu.Lock()
            sm.applyIdx = appMsg.Index
            sm.applyTerm = appMsg.Term

            nxtOp := appMsg.Command.(Op)
            fmt.Println("ApplyCh on shardMaster", sm.me, "nxtOp", nxtOp)
            //Duplicate Detection
            opId, keyFound := sm.clientReqMap[nxtOp.ClientId]
            if !(keyFound && opId == nxtOp.OpId) {
                sm.clientReqMap[nxtOp.ClientId] = nxtOp.OpId
				if nxtOp.Operation == "Join" {
                    fmt.Println("ApplyCh on shardMaster", sm.me, "Applying Join", "nxtOp.Servers", nxtOp.Servers)
					prvIdx := len(sm.configs)-1
					newgroups := make(map[int][]string)
                    var newshards [NShards]int
					RevMap := make(map[int][]int)
					var oldClusters []int

					if prvIdx != 0 {
                        oldgroups := sm.configs[prvIdx].Groups
						for key, value := range oldgroups {
							newgroups[key] = value
							oldClusters = append(oldClusters, key)
						}
                        oldshards := sm.configs[prvIdx].Shards
						for i, value := range oldshards {
							_, ok := RevMap[value]
							if ok {
								RevMap[value] = append(RevMap[value], i)
							} else {
								shards := []int{i}
								RevMap[value] = shards
							}
						}
					}
                    fmt.Println("ApplyCh on shardMaster", sm.me, "Applying Join", "oldgroups", newgroups, "oldshards", sm.configs[prvIdx].Shards, "oldClusters", oldClusters, "Old RevMap", RevMap)

					var newClusters []int
					for key, value := range nxtOp.Servers {
                        _, ok := newgroups[key]
                        if !ok {
						    newgroups[key] = value
						    newClusters = append(newClusters, key)
                        }
					}

                    if len(newClusters) == 0 {
                        fmt.Println("ApplyCh on shardMaster", sm.me, "Applying Join. No new clusters to add")
                        sm.mu.Unlock()
                        continue
                    }

                    newLimit := NShards / (len(newClusters) + len(oldClusters))
                    if newLimit < 1 {
                        newLimit = 1
                    } else if newLimit > 10 {
                        newLimit = 10
                    }

                    fmt.Println("ApplyCh on shardMaster", sm.me, "Applying Join", "newgroups", newgroups, "newClusters", newClusters, "newLimit", newLimit)

					positions := []int{}
                    if len(RevMap) > 0 {
						sort.Ints(oldClusters)
						for _, key := range oldClusters {
	    					value := RevMap[key]
    						if (len(positions) < len(newClusters)*newLimit) && (len(value) > newLimit) {
			    				positions = append(positions, value[newLimit:]...)
		    					value = value[:newLimit]
	    						RevMap[key] = value
    						}
    					}
                    } else {
                        idx := 0
                        for idx < 10 {
                            positions = append(positions, idx)
                            idx += 1
                        }
                    }

					sort.Ints(positions)
                    fmt.Println("ApplyCh on shardMaster", sm.me, "Applying Join", "empty positions in RevMap", positions)

					for _, value := range newClusters {
                        posSlice := []int{}
                        if len(positions) > 0 {
						    if newLimit < len(positions){
                            	posSlice = append(posSlice, positions[:newLimit]...)
						    	positions = positions[newLimit:]
							} else {
								posSlice = append(posSlice, positions...)
								positions = positions[:0]
							}
                        }
                        RevMap[value] = posSlice
					}

					finalClusters := append(oldClusters, newClusters...)
					sort.Ints(finalClusters)
                    if len(positions) > 0 {
                        pidx := 0
						for pidx < len(positions) {
							for _, key := range finalClusters {
    	                        if pidx >= len(positions) {
        	                        break
            	                }
								value :=  RevMap[key]
                    	        value = append(value, positions[pidx])
                        	    RevMap[key] = value
                            	pidx += 1
                        	}
						}
                    }

                    fmt.Println("ApplyCh on shardMaster", sm.me, "Applying Join", "New RevMap", RevMap)

					for key, value := range RevMap {
						for _, idx := range value {
							newshards[idx] = key
						}
					}

                    fmt.Println("ApplyCh on shardMaster", sm.me, "Applying Join", "New Num", prvIdx+1, "New Shards", newshards, "New Groups", newgroups)

					newconfig := Config{
						Num		: prvIdx+1,
						Shards	: newshards,
						Groups	: newgroups}

					sm.configs = append(sm.configs, newconfig)
                } else if nxtOp.Operation == "Leave" {
                    fmt.Println("ApplyCh on shardMaster", sm.me, "Applying Leave", "nxtOp.GIDs", nxtOp.GIDs)
					prvIdx := len(sm.configs)-1
					newgroups := make(map[int][]string)
                    var newshards [NShards]int
					RevMap := make(map[int][]int)
					var oldClusters []int

                    if prvIdx == 0 {
                        sm.mu.Unlock()
                        continue
                    }

                    oldgroups := sm.configs[prvIdx].Groups
					for key, value := range oldgroups {
						newgroups[key] = value
						oldClusters = append(oldClusters, key)
					}
                    oldshards := sm.configs[prvIdx].Shards
					for i, value := range oldshards {
						_, ok := RevMap[value]
						if ok {
							RevMap[value] = append(RevMap[value], i)
						} else {
							shards := []int{i}
							RevMap[value] = shards
						}
					}

                    fmt.Println("ApplyCh on shardMaster", sm.me, "Applying Leave", "oldgroups", newgroups, "oldshards", sm.configs[prvIdx].Shards, "oldClusters", oldClusters, "RevMap", RevMap)

					var remClusters []int
					for _, key := range nxtOp.GIDs {
                        _, ok := newgroups[key]
                        if ok {
						    delete(newgroups, key)
						    remClusters = append(remClusters, key)
                        }
					}

                    if len(remClusters) == 0 {
                        fmt.Println("ApplyCh on shardMaster", sm.me, "Applying Leave. No clusters there to remove")
                        sm.mu.Unlock()
                        continue
                    }

					newLimit := NShards / (len(oldClusters) - len(remClusters))
                    if newLimit < 1 {
                        newLimit = 1
                    } else if newLimit > 10 {
                        newLimit = 10
                    }

                    fmt.Println("ApplyCh on shardMaster", sm.me, "Applying Leave", "newgroups", newgroups, "remClusters", remClusters, "newLimit", newLimit)

					positions := []int{}
					for _, cluster := range remClusters {
						positions = append(positions, RevMap[cluster]...)
						delete(RevMap, cluster)
					}

					sort.Ints(positions)
                    fmt.Println("ApplyCh on shardMaster", sm.me, "Applying Leave", "empty positions in RevMap", positions)

					var finalClusters []int
					for _, ocl := range oldClusters {
						found := false
						for _, rcl := range remClusters {
							if ocl == rcl {
								found = true
								break
							}
						}
						if !found {
							finalClusters = append(finalClusters, ocl)
						}
					}

					sort.Ints(finalClusters)

					for _, key := range finalClusters {
						value := RevMap[key]
						newLen := newLimit - len(value)
						if len(positions) > 0 && newLen > 0 {
                       		posSlice := []int{}
                        	posSlice = append(posSlice, value...)
							posSlice = append(posSlice, positions[:newLen]...)
							RevMap[key] = posSlice
							positions = positions[newLen:]
						}
					}

                    if len(positions) > 0 {
                        pidx := 0
						for pidx < len(positions) {
	                        for _, key := range finalClusters {
    	                        if pidx >= len(positions) {
        	                        break
            	                }
                	            value :=  RevMap[key]
                    		    value = append(value, positions[pidx])
                            	RevMap[key] = value
                            	pidx += 1
                        	}
						}
                    }

                    fmt.Println("ApplyCh on shardMaster", sm.me, "Applying Leave", "New RevMap", RevMap)

					for key, value := range RevMap {
						for _, idx := range value {
							newshards[idx] = key
						}
					}

                    fmt.Println("ApplyCh on shardMaster", sm.me, "Applying Leave", "New Num", prvIdx+1, "New Shards", newshards, "New Groups", newgroups)
					newconfig := Config{
						Num		: prvIdx+1,
						Shards	: newshards,
						Groups	: newgroups}

					sm.configs = append(sm.configs, newconfig)
                } else if nxtOp.Operation == "Move" {
                    fmt.Println("ApplyCh on shardMaster", sm.me, "Applying Move")
					prvIdx := len(sm.configs)-1
					newgroups := make(map[int][]string)
                    var newshards [NShards]int

                    if prvIdx == 0 {
                        sm.mu.Unlock()
                        continue
                    }

                    oldgroups := sm.configs[prvIdx].Groups
					for key, value := range oldgroups {
						newgroups[key] = value
					}
					oldshards := sm.configs[prvIdx].Shards
					for idx, value := range oldshards {
						newshards[idx] = value
					}
                    fmt.Println("ApplyCh on shardMaster", sm.me, "Applying Move", "Old Num", prvIdx, "Old Shards", newshards, "Old Groups", newgroups)
                    fmt.Println("ApplyCh on shardMaster", sm.me, "Applying Move", "Change Shard:", nxtOp.Shard, " to Cluster:", nxtOp.GID)
					newshards[nxtOp.Shard] = nxtOp.GID

                    fmt.Println("ApplyCh on shardMaster", sm.me, "Applying Move", "New Num", prvIdx+1, "New Shards", newshards, "New Groups", newgroups)
					newconfig := Config{
					    Num		: prvIdx+1,
					    Shards	: newshards,
					    Groups	: newgroups}

					sm.configs = append(sm.configs, newconfig)
				} else if nxtOp.Operation == "Query" {
                    fmt.Println("ApplyCh on shardMaster", sm.me, "Applying Query")
				}
            }
            sm.mu.Unlock()
        }
    }()
	return sm
}
