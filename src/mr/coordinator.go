package mr

import (
	"errors"
	"fmt"
	"log"
	"math/rand"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.
	Tasks   []Task
	Workers map[int]bool
	//ReduceTask []Task
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) FetchTask(workerID int, task *Task) error {
	//verify workers exists if not fail, workers are not allowed to self-assign
	if !c.Workers[workerID] {
		return errors.New("unregistered ID, please call RegisterWorker")
	}

	for i, t := range c.Tasks {
		if t.State() == StateIdle {
			//TODO having to "copy" values manually is annoying, is there a better way
			task.Filename = t.Filename
			task.TaskID = t.TaskID
			c.Tasks[i].WorkerID = workerID
			c.Tasks[i].state = StateInProgress
			return nil
		}
	}
	return errors.New("no idle task")
}

func (c *Coordinator) CompleteTask(completedTask Task, reply *Task) error {
	task := &c.Tasks[completedTask.TaskID]
	if task.WorkerID != completedTask.WorkerID {
		return fmt.Errorf("only assigned worker can complete task")
	}
	if task.State() != StateInProgress {
		return fmt.Errorf("only a task InProgress can be marked completed")
	}
	task.state = StateCompleted
	return nil

}

//
// start a thread that listens for RPCs from worker.go
//
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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce Tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	// Your code here.
	numberOfTasks := len(files) + nReduce
	c.Tasks = make([]Task, numberOfTasks)
	c.Workers = make(map[int]bool)
	for i := range files {
		c.Tasks[i].Filename = files[i]
		c.Tasks[i].TaskID = i
		c.Tasks[i].TaskType = TaskTypeMap
	}

	for _, t := range c.Tasks[len(files):] {
		t.TaskType = TaskTypeReduce
	}
	fmt.Println("coordinator created")
	c.server()
	return &c
}

//will break if >99 workers are registered
func (c *Coordinator) RegisterWorker(args ExampleArgs, reply *int) error {
	rand.Seed(time.Now().UnixNano())
	var id int
	//generate random id between 1 and 99
	for id == 0 {
		id = rand.Intn(99) + 1
	}

	*reply = id
	c.Workers[id] = true
	return nil
}
