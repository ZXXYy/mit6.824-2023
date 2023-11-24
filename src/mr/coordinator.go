package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"time"
)

type Status int32

const (
	Ready   Status = 0
	Process Status = 1
	Done    Status = 2
)

type FileStatus struct {
	status    Status
	timestamp int64
}

type Coordinator struct {
	// Your definitions here.
	files             map[string]FileStatus
	intermediateFiles map[string]FileStatus
	nReduce           int
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) AssignTask(args *TaskArgs, reply *TaskReply) error {
	cnt := 0
	// deal with crash
	for file, status := range c.files {
		if status.status == Process && time.Now().Unix()-status.timestamp > 10 {
			c.files[file] = FileStatus{Ready, 0}
		}
	}
	for file, status := range c.intermediateFiles {
		if status.status == Process && time.Now().Unix()-status.timestamp > 10 {
			c.intermediateFiles[file] = FileStatus{Ready, 0}
		}
	}

	for file, status := range c.files {
		if status.status == Ready {
			reply.TaskType = "map"
			reply.TaskFile = file
			reply.NReduce = c.nReduce
			reply.Nmap = len(c.files)
			reply.MapTaskNum = cnt
			reply.ReduceTaskNum = 0
			c.files[file] = FileStatus{Process, time.Now().Unix()}
			fmt.Printf("assign map task %s\n", file)
			return nil
		}
		cnt++
	}
	cnt = 0
	for file, status := range c.intermediateFiles {
		if status.status == Ready {
			reply.TaskType = "reduce"
			reply.TaskFile = file
			reply.NReduce = c.nReduce
			reply.Nmap = len(c.files)
			reply.MapTaskNum = 0
			reply.ReduceTaskNum = cnt
			c.intermediateFiles[file] = FileStatus{Process, time.Now().Unix()}
			return nil
		}
		cnt++
	}
	return nil
}

func (c *Coordinator) FinishTask(args *FinishArgs, reply *FinishReply) error {
	if args.TaskType == "map" {
		c.files[args.TaskFile] = FileStatus{Done, time.Now().Unix()}
	} else if args.TaskType == "reduce" {
		c.intermediateFiles[args.TaskFile] = FileStatus{Done, time.Now().Unix()}
	}
	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	// Your code here.
	for _, status := range c.files {
		if status.status != Done {
			return false
		}
	}
	for _, status := range c.intermediateFiles {
		if status.status != Done {
			return false
		}
	}

	return true
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		files:             make(map[string]FileStatus),
		intermediateFiles: make(map[string]FileStatus),
		nReduce:           nReduce,
	}
	for _, file := range files {
		c.files[file] = FileStatus{Ready, 0}
	}
	for i := 0; i < nReduce; i++ {
		c.intermediateFiles[strconv.Itoa(i)] = FileStatus{Ready, 0}
	}

	// Your code here.

	c.server()
	return &c
}
