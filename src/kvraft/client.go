package raftkv

import "labrpc"
import "crypto/rand"
import "math/big"
import "fmt"

type Clerk struct {
	servers     []*labrpc.ClientEnd
	// You will have to modify this struct.
    clientId    int64
    leaderId    int
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
	// You'll have to add code here.
    ck.clientId = nrand()
    ck.leaderId = 0
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.Get", &args, &reply)
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	opid := nrand()
    for {
        args := GetArgs{Key         : key,
                 		OpId        : opid,
                 		ClientId    : ck.clientId}
        reply := GetReply{}

        ok := ck.servers[ck.leaderId].Call("RaftKV.Get", &args, &reply)
		fmt.Println("Client: ", ck.clientId, "Sent Get, args: ", args, ", reply: ", reply)
        if ok {
            if reply.WrongLeader {
                ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
            } else {
                if reply.Err == OK {
                    return reply.Value
                } else if reply.Err == ErrNoKey {
					return ""
				} else if reply.Err == ErrNoConcensus {
					ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
				}
            }
        }
    }
}

// shared by Put and Append.
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.PutAppend", &args, &reply)
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	opid := nrand()
    for {
        args := PutAppendArgs{Key         : key,
				 			  Value		  : value,
				 			  Op	      : op,
                 			  OpId        : opid,
                   			  ClientId    : ck.clientId}
        reply := PutAppendReply{}

        ok := ck.servers[ck.leaderId].Call("RaftKV.PutAppend", &args, &reply)
		fmt.Println("Client: ", ck.clientId, "Sent PutAppend, args: ", args, ", reply: ", reply)

        if ok {
            if reply.WrongLeader {
                ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
            } else {
                if reply.Err == OK {
                    return
                } else if reply.Err == ErrNoConcensus {
                    ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
                }
            }
        }
    }
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
