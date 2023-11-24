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
type TaskArgs struct{}
type TaskReply struct {
	NReduce       int    // 告诉worker总共有几个reduce task
	Nmap          int    // 告诉worker总共有几个map task
	MapTaskNum    int    // 告诉worker当前处理的是第几个map task
	ReduceTaskNum int    // 告诉worker当前处理的是第几个reduce task
	TaskType      string // 告诉worker需要处理的task类型是map还是reduce
	TaskFile      string // 告诉worker需要处理的文件是哪个
}

type FinishArgs struct {
	TaskType string // 告诉coordinator当前完成的任务类型
	TaskFile string // 告诉coordinator当前完成的任务对应的文件
}
type FinishReply struct{}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
