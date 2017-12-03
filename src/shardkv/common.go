package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time paxos.
// Shardmaster decides which group serves each shard.
// Shardmaster may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK            = "OK"
	ErrNoKey      = "ErrNoKey"
	ErrWrongGroup = "ErrWrongGroup"
    ErrWrongLeader      = "ErrWrongLeader"
    ErrNoConcensus      = "ErrNoConcensus"
    ErrOldConfig        = "ErrOldConfig"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
    OpId        int64
    ClientId    int64
}

type PutAppendReply struct {
	WrongLeader bool
	Err         Err
}

type GetArgs struct {
	Key string
    OpId        int64
    ClientId    int64
}

type GetReply struct {
	WrongLeader bool
	Err         Err
	Value       string
}

type GetShardsArgs struct {
    ConfigNumber        int
    RequestedShards     map[int]bool
}

type GetShardsReply struct {
    RequestedKVs        map[string][]string
    ClientReqMap        map[int64]int64
    WrongLeader         bool
    Err                 Err
}

type ConfigChange struct {
    Config          shardmaster.Config
    NewKVs          map[string][]string
    ClientReqMap    map[int64]int64
}
