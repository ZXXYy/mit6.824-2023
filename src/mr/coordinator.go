package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
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
	fileMutex         sync.Mutex
	filesIndex        map[string]int
	intermediateFiles map[string]FileStatus
	intermediateIndex map[string]int
	nReduce           int
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) AssignTask(args *TaskArgs, reply *TaskReply) error {
	// deal with crash
	c.fileMutex.Lock()
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
	c.fileMutex.Unlock()
	for file, status := range c.files {
		if status.status == Ready {
			reply.TaskType = "map"
			reply.TaskFile = file
			reply.NReduce = c.nReduce
			reply.Nmap = len(c.files)
			reply.MapTaskNum = c.filesIndex[file]
			reply.ReduceTaskNum = 0
			c.fileMutex.Lock()
			c.files[file] = FileStatus{Process, time.Now().Unix()}
			c.fileMutex.Unlock()
			// fmt.Printf("assign map task %s\n", file)
			return nil
		}
	}
	for _, status := range c.files {
		if status.status == Ready || status.status == Process {
			return nil
		}
	}
	for file, status := range c.intermediateFiles {
		if status.status == Ready {
			reply.TaskType = "reduce"
			reply.TaskFile = file
			reply.NReduce = c.nReduce
			reply.Nmap = len(c.files)
			reply.MapTaskNum = 0
			reply.ReduceTaskNum = c.intermediateIndex[file]
			c.fileMutex.Lock()
			c.intermediateFiles[file] = FileStatus{Process, time.Now().Unix()}
			c.fileMutex.Unlock()
			return nil
		}
	}
	return nil
}

func (c *Coordinator) FinishTask(args *FinishArgs, reply *FinishReply) error {
	c.fileMutex.Lock()
	if args.TaskType == "map" {
		// fmt.Printf("finish map task %s\n", args.TaskFile)
		c.files[args.TaskFile] = FileStatus{Done, time.Now().Unix()}
	} else if args.TaskType == "reduce" {
		c.intermediateFiles[args.TaskFile] = FileStatus{Done, time.Now().Unix()}
		// remove intermediate files
		for i := 0; i < len(c.files); i++ {
			os.Remove("mr-" + strconv.Itoa(i) + "-" + args.TaskFile)
		}
	}
	c.fileMutex.Unlock()
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
		filesIndex:        make(map[string]int),
		intermediateFiles: make(map[string]FileStatus),
		intermediateIndex: make(map[string]int),
		nReduce:           nReduce,
		fileMutex:         sync.Mutex{},
	}
	for idx, file := range files {
		c.files[file] = FileStatus{Ready, 0}
		c.filesIndex[file] = idx
	}
	for i := 0; i < nReduce; i++ {
		c.intermediateFiles[strconv.Itoa(i)] = FileStatus{Ready, 0}
		c.intermediateIndex[strconv.Itoa(i)] = i
	}

	// Your code here.

	c.server()
	return &c
}
