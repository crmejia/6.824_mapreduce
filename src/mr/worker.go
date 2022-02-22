package mr

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
)
import "log"
import "net/rpc"
import "hash/fnv"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

var workerID int

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	var err error
	workerID, err = CallRegisterWorker()
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	for {
		task, err := CallFetchTask()
		if err != nil { //as per the hint this might mean that the work is done and workers can exit
			fmt.Println(err.Error())
			return
		}
		if task.TaskType == TaskTypeMap {
			//open file into a content and close
			contents := LoadFile(task.Filename)
			//call map on content
			intermediate := MapFile(task.Filename, contents, mapf)

			//TODO write intermediate to files as specified buckets
			oname := fmt.Sprintf("IM_%d", task.TaskID)
			ofile, err := os.Create(oname)
			if err != nil {
				fmt.Println(err.Error())
				return
			}
			defer ofile.Close()

			enc := json.NewEncoder(ofile)
			for _, kv := range intermediate {
				if err := enc.Encode(kv); err != nil {
					log.Fatal(err.Error())
				}
			}
		}
		err = CallCompleteTask(task)
		if err != nil {
			// Assume the worker took too long. Try to Re-register
			workerID, err = CallRegisterWorker()
			if err != nil {
				//assume the coordinator is Done, exit
				return
			}

		}
	}
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
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

func CallRegisterWorker() (int, error) {
	var id int
	ok := call("Coordinator.RegisterWorker", nil, &id)
	if ok {
		return id, nil
	}
	return 0, errors.New("unable to register worker")
}
func CallFetchTask() (Task, error) {
	args := workerID
	task := Task{}

	ok := call("Coordinator.FetchTask", args, &task)
	if ok {
		return task, nil
	}
	return task, errors.New("unable to fetch task")
}

func CallCompleteTask(completedTask Task) error {
	reply := Task{}
	ok := call("Coordinator.CompleteTask", completedTask, &reply)
	if ok {
		return nil
	}
	return errors.New("unable to complete task")
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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

func LoadFile(filename string) string {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}

	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	return string(content)
}

func MapFile(filename, contents string, mapf func(string, string) []KeyValue) []KeyValue {
	return mapf(filename, contents)
}
