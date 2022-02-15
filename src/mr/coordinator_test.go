package mr_test

import (
	"6.824/mr"
	"testing"
)

func TestFetchTask(t *testing.T) {
	want := "filename"
	coordinator := mr.Coordinator{
		Tasks: []mr.Task{
			mr.Task{Filename: want},
		},
	}
	task := mr.Task{}
	err := coordinator.FetchTask(&mr.ExampleArgs{X: 0}, &task)
	if err != nil {
		t.Fatal()
	}
	got := task.Filename
	if want != got {
		t.Errorf("want %s, got %s", want, got)
	}
}
