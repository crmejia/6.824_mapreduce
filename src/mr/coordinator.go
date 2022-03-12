package mr

import (
	"errors"
	"fmt"
	"log"
	"math/rand"
	"strconv"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.
	MapTasks   []Task
	ReduceTask []Task
	Workers    map[int]bool
	mu         sync.Mutex
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

//will break if >99 workers are registered
func (c *Coordinator) RegisterWorker(args ExampleArgs, reply *int) error {
	rand.Seed(time.Now().UnixNano())
	var id int
	//generate random id between 1 and 99
	for id == 0 && !c.Workers[id] {
		id = rand.Intn(99) + 1
	}

	*reply = id
	c.mu.Lock()
	c.Workers[id] = true
	c.mu.Unlock()
	return nil
}

func (c *Coordinator) FetchTask(workerID int, task *Task) error {
	//verify workers exists if not fail, workers are not allowed to self-assign
	if !c.Workers[workerID] {
		return errors.New("unregistered ID, please call RegisterWorker")
	}
	mapDone := true //flag that marks if map tasks are done

	c.mu.Lock()
	defer c.mu.Unlock()

	//TODO iterate over state of all task instead of task themselves
	for i, t := range c.MapTasks {
		if t.State == StateIdle {
			//TODO having to "copy" values manually is annoying, is there a better way
			task.Filename = t.Filename
			task.TaskID = t.TaskID
			task.WorkerID = workerID
			task.TaskType = t.TaskType
			task.NReduce = t.NReduce
			c.MapTasks[i].WorkerID = workerID
			c.MapTasks[i].State = StateInProgress
			c.MapTasks[i].StartTime = time.Now()
			mapDone = false
			return nil
		} else if t.State == StateInProgress {
			mapDone = false
		}
	}

	for i, t := range c.ReduceTask {
		if t.State == StateIdle && mapDone {
			task.Filename = t.Filename
			task.TaskID = t.TaskID
			task.WorkerID = workerID
			task.TaskType = t.TaskType
			task.MMap = t.MMap
			task.NReduce = t.NReduce
			c.ReduceTask[i].WorkerID = workerID
			c.ReduceTask[i].State = StateInProgress
			c.ReduceTask[i].StartTime = time.Now()
			return nil
		}
	}

	return errors.New("no idle task")
}

func (c *Coordinator) CompleteTask(completedTask Task, reply *Task) error {
	var task *Task
	//lookup the task on the appropriate list
	if completedTask.TaskType == TaskTypeMap {
		if completedTask.TaskID > len(c.MapTasks) {
			return errors.New("out of bounds id")
		}
		task = &c.MapTasks[completedTask.TaskID]
	} else { //reduce tasks
		if completedTask.TaskID > len(c.ReduceTask) {
			return errors.New("out of bounds id")
		}
		task = &c.ReduceTask[completedTask.TaskID]
	}

	if task.WorkerID != completedTask.WorkerID {
		return fmt.Errorf("only assigned worker can complete task")
	}
	if task.State != StateInProgress {
		return fmt.Errorf("only a task InProgress can be marked completed")
	}

	c.mu.Lock()
	task.State = StateCompleted
	c.mu.Unlock()
	//c.MapTasks[completedTask.TaskID].State = StateCompleted
	return nil

}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	server := rpc.NewServer()
	err := server.Register(c)
	if err != nil {
		log.Fatal(err)
	}
	rand.Seed(time.Now().UnixNano())
	id := rand.Int()
	rpcPath := fmt.Sprintf("/_goRPC_%d", id)
	rpcDebugPath := fmt.Sprintf("/debug/rpc_%d", id)
	server.HandleHTTP(rpcPath, rpcDebugPath)

	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	sockname += strconv.Itoa(id)
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
	ret := true

	// Your code here.
	for _, t := range c.ReduceTask {
		if t.State != StateCompleted {
			ret = false
			break
		}
	}

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// NReduce is the number of reduce MapTasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	// Your code here.
	mMap := len(files)
	c.MapTasks = make([]Task, mMap)
	c.Workers = make(map[int]bool)
	for i := range c.MapTasks {
		c.MapTasks[i].TaskType = TaskTypeMap
		c.MapTasks[i].TaskID = i
		c.MapTasks[i].MMap = mMap
		c.MapTasks[i].NReduce = nReduce
		c.MapTasks[i].Filename = files[i]
	}

	c.ReduceTask = make([]Task, nReduce)
	for i := range c.ReduceTask {
		c.ReduceTask[i].TaskType = TaskTypeReduce
		c.ReduceTask[i].TaskID = i
		c.ReduceTask[i].MMap = mMap
		c.ReduceTask[i].NReduce = nReduce
	}
	c.server()
	go c.checkTask()
	return &c
}

//this is an interesting case for testing. This is an infinite loop that sleeps forever
//my solution was to separate the actual logic from the infinite loop.
func (c *Coordinator) checkTask() {
	for {
		c.CheckTask()
		time.Sleep(500 * time.Millisecond)
	}
}

//loop over in progress task and reset long running jobs
func (c *Coordinator) CheckTask() {
	for i, t := range c.MapTasks {
		if t.State == StateInProgress {
			now := time.Now()
			elapsed := now.Sub(t.StartTime)
			if elapsed > (10 * time.Second) {
				//resetting task
				c.mu.Lock()
				//t.State = StateIdle doesn't modify
				c.MapTasks[i].State = StateIdle
				wID := c.MapTasks[i].WorkerID
				c.MapTasks[i].WorkerID = 0
				c.Workers[wID] = false
				c.mu.Unlock()
			}
		}
	}
	for i, t := range c.ReduceTask {
		if t.State == StateInProgress {
			now := time.Now()
			elapsed := now.Sub(t.StartTime)
			if elapsed > (10 * time.Second) {
				//resetting task
				c.mu.Lock()
				//t.State = StateIdle doesn't modify
				c.ReduceTask[i].State = StateIdle
				wID := c.ReduceTask[i].WorkerID
				c.ReduceTask[i].WorkerID = 0
				c.Workers[wID] = false
				c.mu.Unlock()
			}
		}
	}
}
