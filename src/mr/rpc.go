package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"time"
)
import "strconv"

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
type State int

const (
	StateIdle State = iota
	StateInProgress
	StateCompleted
)

var validState = map[State]bool{
	StateIdle:       true,
	StateInProgress: true,
	StateCompleted:  true,
}

type TaskType int

const (
	TaskTypeMap TaskType = iota
	TaskTypeReduce
)

// Add your RPC definitions here.
type Task struct {
	Filename string
	state    State
	//TaskID int this might need to be set at some point to coordinate workers properly
	//for the moment setting a taskID to be used as the
	TaskID    int
	TaskType  TaskType
	WorkerID  int
	StartTime time.Time
}

func (t Task) State() State {
	return t.state
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
