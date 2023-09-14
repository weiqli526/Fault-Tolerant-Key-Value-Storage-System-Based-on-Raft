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

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
type TaskRequesetArgs struct {
	RequestType int // 0: ask for task 1: inform of accomplishment
	TaskType    int // 0: map task, 1: reduce task 2: wait
	TaskId      int
}

type TaskReplyArgs struct {
	InputFile string
	X         int // task number
	TaskType  int // 0: map task, 1: reduce task
	NReduce   int
	NMap      int
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
