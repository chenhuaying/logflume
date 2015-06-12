package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestNew(t *testing.T) {
	path := "./retry/example.log"
	r, err := NewRetryer(path)
	if err != nil {
		t.Log("New example log failed, error: %s", err)
		t.Fail()
	}
	if r.fileName != path {
		t.Fail()
	}

	if _, err := os.Stat(path); os.IsNotExist(err) {
		t.Fail()
	}
	os.RemoveAll(filepath.Dir(path))
}

func TestRotate(t *testing.T) {
	path := "./retry-test/example.log"
	r, _ := NewRetryer(path)
	r.suffix = "20150610"
	r.file.Write([]byte("old 1\n"))
	r.file.Write([]byte("old 2\n"))
	r.doBackup("a new\n")
	if _, err := os.Stat(path + "." + "20150610"); os.IsNotExist(err) {
		t.Fail()
	}
	os.RemoveAll(filepath.Dir(path))
}
