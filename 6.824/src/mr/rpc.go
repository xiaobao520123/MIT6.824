package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

const (
	TaskMap = 1
	TaskReduce
)

const (
	StateTaskIdle     = iota
	StateTaskStart    = 1
	StateTaskFinished = 2
)

const (
	StateIdle = iota
	StateMapping
	StateReducing
	StateDone
)

const (
	ErrorSuccess = iota
	ErrorNoTask
	ErrorTaskAlreadyFinished
	ErrorInvalidTaskType
)

// Add your RPC definitions here.
type AskTaskArgs struct {
}

type AskTaskReply struct {
	ErrCode   int
	TaskIndex int
	TaskName  string
	TaskType  int // 1=Map 2=Reduce

	// args for map
	Filename string
	NReduce  int

	// args for reduce
	Filenames []string
}

type FinishTaskArgs struct {
	TaskName string
	TaskType int // 1=Map 2=Reduce
}

type FinishTaskReply struct {
	ErrCode int
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
