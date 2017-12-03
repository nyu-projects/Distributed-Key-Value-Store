package shardmaster

//
// Shardmaster clerk.
//

import "labrpc"
import "time"
import "crypto/rand"
import "math/big"
//import "fmt"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
    clientId    int64
//    leaderId    int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// Your code here.
    ck.clientId = nrand()
//    ck.leaderId = 0
	return ck
}

func (ck *Clerk) Query(num int) Config {
	opid := nrand()
	args := &QueryArgs{}
	// Your code here.
	args.Num = num
	args.OpId = opid
	args.ClientId = ck.clientId
	for {
		// try each known server.
		for _, srv := range ck.servers {
		    var reply QueryReply
		    //fmt.Println("Client: ", ck.clientId, "Sending Query, args: ", args)
		    ok := srv.Call("ShardMaster.Query", args, &reply)
		    //fmt.Println("Client: ", ck.clientId, "Query, reply: ", reply)
            if ok && reply.WrongLeader == false {
                return reply.Config
            }
/*
		if !ok {
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
		} else if reply.WrongLeader {
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
		} else if reply.Err != OK {
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
		} else {
			return reply.Config
		}
*/
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
    opid := nrand()
	args := &JoinArgs{}
	// Your code here.
	args.Servers = servers
	args.OpId = opid
    args.ClientId = ck.clientId

    for {
        // try each known server.
        for _, srv := range ck.servers {
            var reply JoinReply
		    //fmt.Println("Client: ", ck.clientId, "Sending Join, args: ", args)
            ok := srv.Call("ShardMaster.Join", args, &reply)
		    //fmt.Println("Client: ", ck.clientId, "Join, reply: ", reply)
            if ok && reply.WrongLeader == false {
                return
            }
/*
        if !ok {
            ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
        } else if reply.WrongLeader {
            ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
        } else if reply.Err != OK {
            ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
        } else {
            return
        }
*/
        }
        time.Sleep(100 * time.Millisecond)
    }
}

func (ck *Clerk) Leave(gids []int) {
    opid := nrand()
	args := &LeaveArgs{}
	// Your code here.
	args.GIDs = gids
    args.OpId = opid
    args.ClientId = ck.clientId

    for {
        // try each known server.
        for _, srv := range ck.servers {
            var reply LeaveReply
		    //fmt.Println("Client: ", ck.clientId, "Sending Leave, args: ", args)
            ok := srv.Call("ShardMaster.Leave", args, &reply)
		    //fmt.Println("Client: ", ck.clientId, "Leave, reply: ", reply)
            if ok && reply.WrongLeader == false {
                return
            }
/*
        if !ok {
            ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
        } else if reply.WrongLeader {
            ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
        } else if reply.Err != OK {
            ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
        } else {
            return
        }
*/
        }
        time.Sleep(100 * time.Millisecond)
    }
}

func (ck *Clerk) Move(shard int, gid int) {
    opid := nrand()
	args := &MoveArgs{}
	// Your code here.
	args.Shard = shard
	args.GID = gid
    args.OpId = opid
    args.ClientId = ck.clientId

    for {
        // try each known server.
        for _, srv := range ck.servers {
            var reply MoveReply
		    //fmt.Println("Client: ", ck.clientId, "Sending Move, args: ", args)
            ok := srv.Call("ShardMaster.Move", args, &reply)
		    //fmt.Println("Client: ", ck.clientId, "Move, reply: ", reply)
            if ok && reply.WrongLeader == false {
                return
            }
/*
        if !ok {
            ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
        } else if reply.WrongLeader {
            ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
        } else if reply.Err != OK {
            ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
        } else {
            return
        }
*/
        }
        time.Sleep(100 * time.Millisecond)
    }
}
