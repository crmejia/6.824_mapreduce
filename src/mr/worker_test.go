package mr_test

import (
	"6.824/mr"
	"bytes"
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

func TestHashIntermediatesIntoNSlices(t *testing.T) {
	nReduce := 5
	intermediate := []mr.KeyValue{
		{Key: "0", Value: "0"},
		{Key: "1", Value: "1"},
		{Key: "2", Value: "2"},
		{Key: "3", Value: "3"},
		{Key: "4", Value: "4"},
		{Key: "5", Value: "5"},
		{Key: "6", Value: "6"},
		{Key: "7", Value: "7"},
		{Key: "8", Value: "8"},
	}

	nIntermediates := mr.HashIntermediates(nReduce, intermediate)

	got := len(nIntermediates)
	if nReduce != got {
		t.Errorf("want %d NReduce buckets, got %d", nReduce, got)
	}
}

func TestMapTaskCreatesNReduceFiles(t *testing.T) {
	bucket := []mr.KeyValue{
		{Key: "0", Value: "0"},
		{Key: "1", Value: "1"},
	}

	want := "{\"Key\":\"0\",\"Value\":\"0\"}\n{\"Key\":\"1\",\"Value\":\"1\"}\n"
	//{"Key":"A","Value":"1"}
	//{"Key":"A","Value":"1"}
	buffer := bytes.Buffer{} //io.writer
	mr.WriteReduceFiles(bucket, &buffer)

	got := string(buffer.Bytes())
	if want != got {
		t.Errorf("want %q, got %q", want, got)
	}
}

//TODO test CreateReduceFileError
func TestLoadReduceFileIntoIntermediate(t *testing.T) {

	want := []mr.KeyValue{
		{Key: "0", Value: "0"},
		{Key: "1", Value: "1"},
	}
	validJSON := []byte("{\"Key\":\"0\",\"Value\":\"0\"}\n{\"Key\":\"1\",\"Value\":\"1\"}\n")
	buffer := bytes.Buffer{}
	buffer.Write(validJSON)
	got := mr.ReadReduceFile(&buffer)
	if !cmp.Equal(want, got) {
		t.Errorf("want %q, got %q", want, got)
	}
}
