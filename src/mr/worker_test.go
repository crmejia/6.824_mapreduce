package mr_test

import (
	"6.824/mr"
	"bytes"
	"github.com/google/go-cmp/cmp"
	"testing"
)

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
	mr.EncodeReduceFiles(bucket, &buffer)

	got := string(buffer.Bytes())
	if want != got {
		t.Errorf("want %q, got %q", want, got)
	}
}

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
