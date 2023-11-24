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

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	for true {
		// Your worker implementation here.
		task, taskfile, nreduce, nmap, mapTaskNum, reduceTaskNum := AvailableForTask()
		if task == "map" {
			// read taskfile
			file, err := os.Open("./" + taskfile)
			if err != nil {
				fmt.Printf("cannot open %v", taskfile)
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
			// write intermediate files
			for _, kv := range kva {
				reducefilenum := strconv.Itoa(ihash(kv.Key) % nreduce)
				intermediate := "mr-" + strconv.Itoa(mapTaskNum) + "-" + reducefilenum
				f, err := os.OpenFile(intermediate, os.O_CREATE|os.O_WRONLY, 0644)
				if err != nil {
					log.Fatalf("cannot open %v", intermediate)
				}
				enc := json.NewEncoder(f)
				err = enc.Encode(&kv)
				intermediatefiles = append(intermediatefiles, intermediate)
			}
			CallFinishTask(task, taskfile, intermediatefiles)
		} else if task == "reduce" {
			intermediate := []KeyValue{}
			for i := 0; i < nmap; i++ {
				intermediatefile := "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(reduceTaskNum)
				file, err := os.Open(intermediatefile)
				if err != nil {
					log.Fatalf("cannot open %v", intermediatefile)
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
			CallFinishTask(task, strconv.Itoa(reduceTaskNum), []string{})

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
		fmt.Printf("call failed!\n")
		os.Exit(0)
	}
	return "", "", 0, 0, 0, 0
}

func CallFinishTask(taskType string, taskFile string, intermediateFiles []string) {
	// declare an argument structure.
	args := FinishArgs{}
	// declare a reply structure.
	reply := FinishReply{}
	args.TaskType = taskType
	args.TaskFile = taskFile
	args.IntermediateFiles = intermediateFiles
	ok := call("Coordinator.FinishTask", &args, &reply)
	if ok {
		return
	} else {
		fmt.Printf("call failed!\n")
		os.Exit(0)
	}
	return
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
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
