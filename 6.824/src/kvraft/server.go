package kvraft

import (
	"log"
	"sync"
	"sync/atomic"

	"../labgob"
	"../labrpc"
	"../raft"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	RequestId int64
	OptType   string
	Key       string
	Value     string
}

type OpResult struct {
	applied bool
	err     Err
	value   interface{}
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	database map[string]interface{}

	opResult   map[Op]*OpResult
	opMu       sync.Mutex
	raftWaiter *sync.Cond
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	if kv.killed() {
		return
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()

	requestId := args.RequestId
	key := args.Key

	op := Op{}
	op.RequestId = requestId
	op.OptType = "Get"
	op.Key = key
	_, _, isleader := kv.rf.Start(op)
	if !isleader {
		reply.Err = ErrWrongLeader
		reply.Value = ""
		return
	}
	DPrintf("[%v] op get, key=%v, requestId=%v\n", kv.me, args.Key, args.RequestId)

	kv.opMu.Lock()
	if _, ok := kv.opResult[op]; !ok {
		kv.opResult[op] = new(OpResult)
		kv.opResult[op].applied = false
	}
	kv.opMu.Unlock()

	kv.raftWaiter.L.Lock()
	for {
		kv.opMu.Lock()
		applied := kv.opResult[op].applied
		value := kv.opResult[op].value
		err := kv.opResult[op].err
		kv.opMu.Unlock()
		if applied {
			reply.Err = err
			reply.Value = value.(string)
			kv.raftWaiter.L.Unlock()
			break
		} else {
			kv.raftWaiter.Wait()
		}
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	if kv.killed() {
		return
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()

	requestId := args.RequestId
	optType := args.Op
	key := args.Key
	value := args.Value

	op := Op{}
	op.RequestId = requestId
	op.OptType = optType
	op.Key = key
	op.Value = value
	_, _, isleader := kv.rf.Start(op)
	if !isleader {
		reply.Err = ErrWrongLeader
		return
	}
	DPrintf("[%v] op %v, key=%v, value=%v, requestId=%v\n", kv.me, args.Op, args.Key, args.Value, args.RequestId)

	kv.opMu.Lock()
	if _, ok := kv.opResult[op]; !ok {
		kv.opResult[op] = new(OpResult)
		kv.opResult[op].applied = false
	}
	kv.opMu.Unlock()

	kv.raftWaiter.L.Lock()
	for {
		kv.opMu.Lock()
		applied := kv.opResult[op].applied
		err := kv.opResult[op].err
		kv.opMu.Unlock()
		if applied {
			reply.Err = err
			kv.raftWaiter.L.Unlock()
			break
		} else {
			kv.raftWaiter.Wait()
		}
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) listen() {
	DPrintf("[%v] listening...\n", kv.me)
	for msg := range kv.applyCh {
		if kv.killed() {
			return
		}
		if !msg.CommandValid {
			continue
		}
		op := msg.Command.(Op)
		key := op.Key
		DPrintf("[%v] grab a msg, msg=%+v\n", kv.me, msg)
		switch op.OptType {
		case "Get":
			{
				kv.opMu.Lock()
				if kv.opResult[op] == nil {
					kv.opResult[op] = new(OpResult)
				}

				if !kv.opResult[op].applied {
					kv.opResult[op].applied = true
					value, ok := kv.database[key]
					if !ok {
						kv.opResult[op].value = ""
						kv.opResult[op].err = ErrNoKey
						DPrintf("[%v] get but no key, key=%v, database=%+v\n", kv.me, key, kv.database)
					} else {
						kv.opResult[op].value = value
						kv.opResult[op].err = OK
						DPrintf("[%v] get success, key=%v, value=%v\n", kv.me, key, value)
					}
					DPrintf("[%v] operation Get apply successfully!\n", kv.me)
				} else {
					DPrintf("[%v] operation Get already applied, op=%+v\n", kv.me, op)
				}

				kv.opMu.Unlock()
				break
			}
		case "Put":
			{
				value := op.Value
				kv.opMu.Lock()
				if kv.opResult[op] == nil {
					kv.opResult[op] = new(OpResult)
				}

				if !kv.opResult[op].applied {
					kv.database[key] = value

					kv.opResult[op].applied = true
					kv.opResult[op].err = OK
					DPrintf("[%v] put, key=%v, value=%v\n", kv.me, key, value)
					DPrintf("[%v] operation Put apply successfully!\n", kv.me)
				} else {
					DPrintf("[%v] operation Put already applied, op=%+v\n", kv.me, op)
				}

				kv.opMu.Unlock()
				break
			}
		case "Append":
			{
				value := op.Value
				kv.opMu.Lock()
				if kv.opResult[op] == nil {
					kv.opResult[op] = new(OpResult)
				}

				if !kv.opResult[op].applied {
					oldValue, ok := kv.database[key]
					if !ok {
						DPrintf("[%v] append but no key, create new key, key=%v\n", kv.me, key)
						oldValue = ""
					}

					kv.database[key] = oldValue.(string) + value
					DPrintf("[%v] append, key=%v, value=%v\n", kv.me, key, value)

					kv.opResult[op].applied = true
					kv.opResult[op].err = OK
					DPrintf("[%v] operation Append apply successfully!\n", kv.me)
				} else {
					DPrintf("[%v] operation Append already applied, op=%+v\n", kv.me, op)
				}

				kv.opMu.Unlock()
				break
			}
		default:
			{
				DPrintf("[%v] unknown operation %+v\n", kv.me, op)
				break
			}
		}
		kv.raftWaiter.Broadcast()
	}
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.database = make(map[string]interface{})
	kv.opResult = make(map[Op]*OpResult)
	kv.raftWaiter = sync.NewCond(&sync.Mutex{})

	go kv.listen()

	return kv
}
