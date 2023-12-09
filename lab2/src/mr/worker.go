package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func do_map(taskfile string, nreduce int, mapTaskNum int, mapf func(string, string) []KeyValue) {
	// read taskfile
	file, err := os.Open("./" + taskfile)
	if err != nil {
		fmt.Printf("[map] cannot open taskfile %v", taskfile)
		return
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", taskfile)
	}
	file.Close()
	// call mapf
	kva := mapf(taskfile, string(content))

	intermediatefiles := []string{}
	intermediate := make(map[int][]KeyValue)
	// write intermediate files
	for _, kv := range kva {
		intermediate[ihash(kv.Key)%nreduce] = append(intermediate[ihash(kv.Key)%nreduce], kv)
	}
	for i := 0; i < nreduce; i++ {
		intermediatefile := "mr-" + strconv.Itoa(mapTaskNum) + "-" + strconv.Itoa(i)
		// fmt.Printf("write intermediate file %s\n", intermediatefile)
		intermediatefiles = append(intermediatefiles, intermediatefile)
		f, err := os.OpenFile(intermediatefile, os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			log.Fatalf("[map] cannot open intermediatefile %v", intermediatefile)
		}
		enc := json.NewEncoder(f)
		for _, kv := range intermediate[i] {
			enc.Encode(&kv)
		}
		f.Close()
	}
}

func do_reduce(taskfile string, nmap int, reduceTaskNum int, reducef func(string, []string) string) {
	intermediate := []KeyValue{}
	for i := 0; i < nmap; i++ {
		intermediatefile := "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(reduceTaskNum)
		file, err := os.Open(intermediatefile)
		if err != nil {
			log.Fatalf("[reduce] cannot open intermediatefile %v", intermediatefile)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		file.Close()
	}
	sort.Sort(ByKey(intermediate))

	oname := "mr-out-" + strconv.Itoa(reduceTaskNum)
	ofile, _ := os.Create(oname)

	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	//
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	ofile.Close()
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	for true {
		// Your worker implementation here.
		task, taskfile, nreduce, nmap, mapTaskNum, reduceTaskNum := AvailableForTask()
		// fmt.Printf("%s task %s\n", task, taskfile)
		if task == "map" {
			do_map(taskfile, nreduce, mapTaskNum, mapf)
			CallFinishTask(task, taskfile)
		} else if task == "reduce" {
			do_reduce(taskfile, nmap, reduceTaskNum, reducef)
			CallFinishTask(task, strconv.Itoa(reduceTaskNum))
		}
		time.Sleep(time.Second)
	}
}

func AvailableForTask() (string, string, int, int, int, int) {
	// declare an argument structure.
	args := TaskArgs{}
	// declare a reply structure.
	reply := TaskReply{}
	ok := call("Coordinator.AssignTask", &args, &reply)
	if ok {
		return reply.TaskType, reply.TaskFile, reply.NReduce, reply.Nmap, reply.MapTaskNum, reply.ReduceTaskNum
	} else {
		fmt.Printf("call failed! %d\n", time.Now().Unix())
		os.Exit(0)
	}
	return "", "", 0, 0, 0, 0
}

func CallFinishTask(taskType string, taskFile string) {
	// declare an argument structure.
	args := FinishArgs{}
	// declare a reply structure.
	reply := FinishReply{}
	args.TaskType = taskType
	args.TaskFile = taskFile
	ok := call("Coordinator.FinishTask", &args, &reply)
	if ok {
		return
	} else {
		fmt.Printf("call failed! %d\n", time.Now().Unix())
		os.Exit(0)
	}
	return
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}
	fmt.Println(err)
	return false
}
