package mr_test

import (
	"6.824/mr"
	"github.com/google/go-cmp/cmp"
	"testing"
)

// unnecessary to test rpc itself
//func TestGetATaskViaRPC(t *testing.T) {
//
//	want := "filename"
//	got := mr.GetTask()
//
//	if want != got {
//		t.Errorf("want %s, got %s")
//	}
//}

func TestCallMap(t *testing.T) {
	contents := "../mrapps/wc.go"
	filename := "filename"
	want := []mr.KeyValue{{Key: filename, Value: "1"}}
	got := mr.MapFile(filename, contents, mapf)

	if !cmp.Equal(want, got) {
		t.Error(cmp.Diff(want, got))
	}
}

func TestLoadFileIntoContent(t *testing.T) {
	filename := "../main/pg-being_ernest.txt"
	contents := mr.LoadFile(filename)

	if len(contents) == 0 {
		t.Errorf("expected file to be loaded into contents")
	}
}

func mapf(filename, contents string) []mr.KeyValue {
	kva := []mr.KeyValue{}
	kva = append(kva, mr.KeyValue{Key: filename, Value: "1"})
	return kva
}
