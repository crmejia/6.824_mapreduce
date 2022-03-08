package mr

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/rpc"
	"os"
	"sort"
	"strings"
	"time"
)
import "log"
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
		log.Println(err.Error())
		return
	}
	for {
		task, err := CallFetchTask()
		if err != nil { //as per the hint this might mean that the work is done and workers can exit
			//TODO maybe not return but sleep?
			time.Sleep(500 * time.Millisecond)
			//the only errors from fetch task are unregistered worker and no idle task atm
			//fmt.Println(err.Error())
			//return
		} else {
			var renames []fileRename
			if task.TaskType == TaskTypeMap {
				buckets := mapTask(task, mapf)
				renames = writeReduceFiles(buckets, task.TaskID)
			} else { //reduce task
				intermediate := LoadReduceTaskFiles(task)
				buffer := reduceTask(intermediate, reducef)
				renames = writeOutput(buffer, task.TaskID)

				//TODO only do this once reduce is completed
				go removeMapFiles(task)
				//TODO goRemoveReduceFiles
			}

			for _, r := range renames {
				os.Rename(r.tmpFile.Name(), r.name)
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
}

//map and reduce are interesting functions to write. I did not TDD but instead "copied"
// the way sequential-mr does. So now, while refactoring, I've realized that they do more thatn
// one thing. Map task does mapping and writing so I'm splitting it. Reduce does three things:
// loads files, reduce, write file. So once again I'm going to split. Hopefully, I can write a
//write file simple enough that it can be atomic and reusable.
func mapTask(task Task, mapf func(string, string) []KeyValue) [][]KeyValue {
	log.Printf("starting map task %d\n", task.TaskID)
	contents := LoadFile(task.Filename)
	//open file into a content and close
	//call map on content
	//intermediate := MapFile(task.Filename, contents, mapf)
	intermediate := mapf(task.Filename, contents)
	sort.Sort(ByKey(intermediate))
	buckets := HashIntermediates(task.NReduce, intermediate)
	return buckets
}

//for renaming atomically
type fileRename struct {
	tmpFile *os.File
	name    string
}

func writeReduceFiles(buckets [][]KeyValue, taskID int) []fileRename {
	rename := []fileRename{}
	for i, bucket := range buckets {
		oname := fmt.Sprintf("mr-%d-%d", taskID, i) //mr-X-Y
		//TODO todo use tmpfile os.CreateTemp then rename
		ofile, err := os.CreateTemp("", oname)
		if err != nil {
			fmt.Println(err.Error())
			return []fileRename{}
		}
		defer ofile.Close()
		//queue file rename
		fRename := fileRename{tmpFile: ofile, name: oname}
		rename = append(rename, fRename)
		EncodeReduceFiles(bucket, ofile)
	}
	return rename
}

func LoadReduceTaskFiles(task Task) []KeyValue {
	log.Printf("starting reduce task %d\n", task.TaskID)
	//load nReduce files
	intermediate := []KeyValue{}
	var inputFiles []string
	for i := 0; i < task.MMap; i++ {
		iname := fmt.Sprintf("mr-%d-%d", i, task.TaskID)
		inputFiles = append(inputFiles, iname)
		ifile, err := os.Open(iname)
		defer ifile.Close()
		if err != nil {
			fmt.Println(err.Error())
			return intermediate
		}
		bucket := ReadReduceFile(ifile)
		intermediate = append(intermediate, bucket...)
	}
	return intermediate
}

func reduceTask(intermediate []KeyValue, reducef func(string, []string) string) string {
	//oname := fmt.Sprintf("mr-out-%d", task.TaskID)
	//ofile, err := os.Create(oname)
	//if err != nil {
	//	fmt.Println(err.Error())
	//	return
	//}
	//defer ofile.Close()
	//iterate over all similar keys then reduce
	output := strings.Builder{}
	for i := 0; i < len(intermediate); {
		j := i + 1
		for ; j < len(intermediate) && intermediate[j].Key == intermediate[i].Key; j++ {

		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}

		reduceOutput := reducef(intermediate[i].Key, values)
		//write keys to reduceOutput files on each iteration
		fmt.Fprintf(&output, "%v %v\n", intermediate[i].Key, reduceOutput)
		//fmt.Fprintf(output, "%v %v\n", intermediate[i].Key, reduceOutput)
		i = j
	}
	return output.String()
}

func writeOutput(output string, taskID int) []fileRename {
	oname := fmt.Sprintf("mr-out-%d", taskID)
	ofile, err := os.CreateTemp("", oname)
	var rename []fileRename
	if err != nil {
		fmt.Println(err.Error())
		return rename
	}

	defer ofile.Close()
	fmt.Fprintf(ofile, output)
	r := fileRename{
		tmpFile: ofile,
		name:    oname}
	rename = append(rename, r)
	return rename
}

func removeMapFiles(task Task) {
	log.Println("removing intermediate map files")
	for i := 0; i < task.MMap; i++ {
		iname := fmt.Sprintf("mr-%d-%d", i, task.TaskID)
		err := os.Remove(iname)
		if err != nil {
			//if remove fails, just log it and exit
			log.Println(err)
			break
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
	log.Println("registering worker")
	var id int
	ok := call("Coordinator.RegisterWorker", ExampleArgs{}, &id)
	if ok {
		return id, nil
	}
	return 0, errors.New("unable to register worker")
}
func CallFetchTask() (Task, error) {
	log.Println("fetching task")
	args := workerID
	task := Task{}

	ok := call("Coordinator.FetchTask", args, &task)
	if ok {
		return task, nil
	}
	return task, errors.New("unable to fetch task")
}

func CallCompleteTask(completedTask Task) error {
	log.Println("completing task")
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

func HashIntermediates(nReduce int, intermediate []KeyValue) [][]KeyValue {
	hashedIm := make([][]KeyValue, nReduce)
	//for i, _ := range hashedIm {
	//	hashedIm[i] := make([]KeyValue, 0)
	//}
	for _, v := range intermediate {
		targetBucket := ihash(v.Key) % nReduce
		hashedIm[targetBucket] = append(hashedIm[targetBucket], v)
	}
	return hashedIm
}

func EncodeReduceFiles(bucket []KeyValue, w io.Writer) error {
	enc := json.NewEncoder(w)
	for _, kv := range bucket {
		if err := enc.Encode(kv); err != nil {
			log.Fatal(err.Error())
		}
	}
	return nil
}

func ReadReduceFile(r io.Reader) []KeyValue {
	dec := json.NewDecoder(r)
	bucket := []KeyValue{}
	for {
		kv := KeyValue{}
		if err := dec.Decode(&kv); err == io.EOF {
			break
		} else if err != nil {
			log.Fatal(err)
		}
		bucket = append(bucket, kv)
	}
	return bucket
}
