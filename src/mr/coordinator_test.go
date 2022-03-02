package mr_test

import (
	"6.824/mr"
	"testing"
	"time"
)

const workerID = 17

func TestFetchTask(t *testing.T) {
	//t.Parallel()
	want := "filename"
	coordinator := mr.Coordinator{
		Workers: map[int]bool{workerID: true},
		MapTasks: []mr.Task{
			{Filename: want},
		},
	}
	task := mr.Task{}
	err := coordinator.FetchTask(workerID, &task)
	if err != nil {
		t.Fatal()
	}
	got := task.Filename
	if want != got {
		t.Errorf("want %s, got %s", want, got)
	}
}

func TestFetchTaskNoMoreTaskAvailableReturnsError(t *testing.T) {
	//t.Parallel()
	coordinator := mr.Coordinator{
		Workers: map[int]bool{workerID: true},
		MapTasks: []mr.Task{
			{},
		},
	}
	task := mr.Task{}
	coordinator.FetchTask(workerID, &task)
	err := coordinator.FetchTask(0, &task)
	if err == nil {
		t.Errorf("want error when fetching task but not task available, got nil")
	}
}

func TestFetchTaskSetsTaskID(t *testing.T) {
	want := 1
	coordinator := mr.Coordinator{
		Workers: map[int]bool{workerID: true},
		MapTasks: []mr.Task{
			{TaskID: 0},
			{TaskID: 1},
		},
	}
	task := mr.Task{}
	coordinator.FetchTask(workerID, &task)
	coordinator.FetchTask(workerID, &task)
	got := task.TaskID
	if want != got {
		t.Errorf("want %d, got %d", want, got)
	}
}

func TestFetchTaskReturnsReduceTaskIfAllMapTaskAreCompleted(t *testing.T) {
	want := mr.TaskTypeReduce
	coordinator := mr.Coordinator{
		Workers: map[int]bool{workerID: true},
		MapTasks: []mr.Task{
			{State: mr.StateCompleted},
		},
		ReduceTask: []mr.Task{
			{TaskType: mr.TaskTypeReduce},
		},
	}
	task := mr.Task{}
	err := coordinator.FetchTask(workerID, &task)
	if err != nil {
		t.Fatal()
	}
	got := task.TaskType
	if want != got {
		t.Errorf("want %d, got %d", want, got)
	}
}

//no longer needed there is a Register method
//func TestFetchTaskAssignsIDToWorker(t *testing.T) {
//	//want := "filename"
//	coordinator := mr.Coordinator{
//		MapTasks: []mr.Task{
//			mr.Task{},
//		},
//	}
//	task := mr.Task{}
//	err := coordinator.FetchTask(0, &task)
//	if err != nil {
//		t.Fatal()
//	}
//	got := task.WorkerID
//	if got == 0 {
//		t.Errorf("want WorkerID to be set > 0")
//	}
//}

//TestFetchReduceTaskFailsIfMapTasksNotComplete
func TestCompleteTaskSetsStateCompleted(t *testing.T) {
	want := mr.StateCompleted
	coordinator := mr.Coordinator{
		Workers:    map[int]bool{workerID: true},
		MapTasks:   []mr.Task{{WorkerID: workerID, State: mr.StateInProgress}},
		ReduceTask: []mr.Task{{WorkerID: workerID, State: mr.StateInProgress}},
	}
	task := mr.Task{WorkerID: workerID, TaskType: mr.TaskTypeMap}
	coordinator.CompleteTask(task, nil)
	got := coordinator.MapTasks[0].State
	if want != got {
		t.Errorf("want Task.StateCompleted %d, got State %d", want, got)
	}

	task.TaskType = mr.TaskTypeReduce
	coordinator.CompleteTask(task, nil)
	got = coordinator.ReduceTask[0].State
	if want != got {
		t.Errorf("want Task.StateCompleted %d, got State %d", want, got)
	}
}

func TestCompleteTaskFailsForWrongWorker(t *testing.T) {
	want := mr.StateInProgress
	coordinator := mr.Coordinator{
		Workers: map[int]bool{workerID: true},
		MapTasks: []mr.Task{
			{WorkerID: workerID},
		},
	}
	task := mr.Task{}                      //worker ID not set
	coordinator.FetchTask(workerID, &task) //coordinator sets task to inprogress
	err := coordinator.CompleteTask(task, nil)
	if err == nil {
		t.Errorf("want error if wrong worker call CompleteTask, got nil")
	}
	got := coordinator.MapTasks[0].State
	if want != got {
		t.Errorf("want Task.StateInProgress %d, got State %d", want, got)
	}
}

func TestCompleteTaskFailsForNonInProgressState(t *testing.T) {
	coordinator := mr.Coordinator{
		Workers:  map[int]bool{workerID: true},
		MapTasks: []mr.Task{{WorkerID: workerID, State: mr.StateIdle}},
	}
	task := mr.Task{WorkerID: workerID}
	err := coordinator.CompleteTask(task, nil)
	if err == nil {
		t.Errorf("want error if worker call CompleteTask a task that is not InProgress, got nil")
	}
}
func TestMakeCoordinator(t *testing.T) {
	//t.Parallel()
	files := []string{"file", "file"}
	nReduce := 3
	want := len(files) + nReduce
	c := mr.MakeCoordinator(files, nReduce)

	got := len(c.MapTasks)
	if want != got {
		t.Errorf("want %d MapTasks, got %d", want, got)
	}
}

func TestMakeCoordinatorCreatesIdleTasks(t *testing.T) {
	//t.Parallel()
	files := []string{"file", "file"}
	nReduce := 3
	c := mr.MakeCoordinator(files, nReduce)

	for _, task := range c.MapTasks {
		got := task.State
		if mr.StateIdle != got {
			t.Errorf("want %q, got %d", mr.StateIdle, got)
		}
	}

	for _, task := range c.ReduceTask {
		got := task.State
		if mr.StateIdle != got {
			t.Errorf("want %q, got %d", mr.StateIdle, got)
		}
	}
}

func TestMakeCoordinatorSetsTasksType(t *testing.T) {
	//t.Parallel()
	files := []string{"file", "file"}
	nReduce := 3
	c := mr.MakeCoordinator(files, nReduce)

	for _, task := range c.MapTasks {
		got := task.TaskType
		if mr.TaskTypeMap != got {
			t.Errorf("want %q, got %d", mr.TaskTypeMap, got)
		}
	}

	for _, task := range c.ReduceTask {
		got := task.TaskType
		if mr.TaskTypeReduce != got {
			t.Errorf("want %q, got %d", mr.TaskTypeReduce, got)
		}
	}
}

func TestMakeCoordinatorSetsFilenameOnMapTask(t *testing.T) {
	files := []string{"file", "file"}
	nReduce := 3
	c := mr.MakeCoordinator(files, nReduce)

	for _, task := range c.MapTasks {
		got := task.Filename
		if got == "" {
			t.Errorf("want Filename to be set, got empty string")
		}
	}

}

func TestRegisterWorker(t *testing.T) {
	coordinator := mr.Coordinator{
		Workers: map[int]bool{},
	}
	var workerID int
	err := coordinator.RegisterWorker(mr.ExampleArgs{}, &workerID)
	if err != nil {
		t.Fatal()
	}
	if workerID <= 0 {
		t.Errorf("want an ID between 1 and 99, got %d", workerID)
	}
}

//func TestCoordinator loops over tasks and "resets" long running task to idle
func TestCoordinatorResetsStaleTask(t *testing.T) {
	coordinator := mr.Coordinator{
		MapTasks: []mr.Task{
			{
				TaskID:   0,
				TaskType: mr.TaskTypeReduce,
			},
		},
		Workers: map[int]bool{workerID: true},
	}
	task := mr.Task{}
	coordinator.FetchTask(workerID, &task) //set task to in progress
	coordinator.MapTasks[0].StartTime = time.Date(2022, time.January, 1, 10, 10, 10, 10, time.UTC)
	coordinator.CheckTask()
	want := mr.StateIdle
	got := coordinator.MapTasks[0].State
	if want != got {
		t.Errorf("want task to have an Idle State, got %d", got)
	}
}

//test that workers are removed if an assigned task doesn't return. Just like TestCoordinatorResetsStaleTask
// the proble is that there could be a collision if the id is reassigned
// and the old workers comes back.

//test coordinator sets filename of reduce task
