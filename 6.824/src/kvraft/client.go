package kvraft

import (
	"crypto/rand"
	"math/big"
	"time"

	"../labrpc"
)

const (
	ClerkRPCTimeout = time.Millisecond * 1000
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	leaderId int
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
	ck.leaderId = int(nrand()) % len(ck.servers)
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	requestId := time.Now().UnixNano()
	DPrintf("[clerk] Get, key=%v, requestId=%v\n", key, requestId)

	for {
		done := make(chan struct{})
		ok := false
		var err Err
		value := ""

		go func(server int) {
			ok, err, value = ck.CallGet(server, key, requestId)
			done <- struct{}{}
		}(ck.leaderId)

		select {
		case <-done:
			{
				if ok {
					switch err {
					case OK:
						{
							DPrintf("[clerk] get success, key=%v, value=%v\n", key, value)
							return value
						}
					case ErrNoKey:
						{
							DPrintf("[clerk] err no key, key=%v\n", key)
							return ""
						}
					case ErrWrongLeader:
						{
							DPrintf("[clerk] send get request to wrong leader, leaderId=%v, key=%v\n",
								ck.leaderId, key)
							ck.shuffleLeader()
							break
						}
					}
				} else {
					DPrintf("[clerk] send get request rpc failed, leaderId=%v, key=%v\n",
						ck.leaderId, key)
					ck.shuffleLeader()
				}
			}
		case <-time.After(ClerkRPCTimeout):
			{
				DPrintf("[clerk] Get operation timeout, retry! key=%v\n", key)
				ck.shuffleLeader()
			}
		}
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	requestId := time.Now().UnixNano()
	DPrintf("[clerk] %v, key=%v, value=%v, requestId=%v\n", op, key, value, requestId)

	for {
		done := make(chan struct{})
		ok := false
		var err Err

		go func(server int) {
			ok, err = ck.CallPutAppend(server, op, key, value, requestId)
			done <- struct{}{}
		}(ck.leaderId)

		select {
		case <-done:
			{
				if ok {
					switch err {
					case OK:
						{
							DPrintf("[clerk] op %v success, key=%v, value=%v\n", op, key, value)
							return
						}
					case ErrNoKey:
						{
							DPrintf("[clerk] err no key, key=%v\n", key)
							break
						}
					case ErrWrongLeader:
						{
							DPrintf("[clerk] send %v request to wrong leader, leaderId=%v, key=%v\n",
								ck.leaderId, op, key)
							ck.shuffleLeader()
							break
						}
					}
				} else {
					DPrintf("[clerk] send %v request rpc failed, leaderId=%v, key=%v\n",
						ck.leaderId, op, key)
					ck.shuffleLeader()
				}
			}
		case <-time.After(ClerkRPCTimeout):
			{
				DPrintf("[clerk] %v operation timeout, retry! key=%v, value=%v\n", op, key, value)
				ck.shuffleLeader()
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

func (ck *Clerk) CallGet(server int, key string, requestId int64) (bool, Err, string) {
	args := &GetArgs{}
	reply := &GetReply{}

	args.Key = key
	args.RequestId = requestId

	ok := ck.sendGet(server, args, reply)
	if !ok {
		return false, "", ""
	}

	return true, reply.Err, reply.Value
}

func (ck *Clerk) CallPutAppend(server int,
	op string, key string, value string, requestId int64) (bool, Err) {

	args := &PutAppendArgs{}
	reply := &PutAppendReply{}

	args.Op = op
	args.Key = key
	args.Value = value
	args.RequestId = requestId

	ok := ck.sendPutAppend(server, args, reply)
	if !ok {
		return false, ""
	}

	return true, reply.Err
}

func (ck *Clerk) sendGet(server int, args *GetArgs, reply *GetReply) bool {
	return ck.servers[server].Call("KVServer.Get", args, reply)
}

func (ck *Clerk) sendPutAppend(server int, args *PutAppendArgs, reply *PutAppendReply) bool {
	return ck.servers[server].Call("KVServer.PutAppend", args, reply)
}

func (ck *Clerk) shuffleLeader() {
	ck.leaderId = int(nrand() % int64(len(ck.servers)))
}
