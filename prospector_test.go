package main

import (
	"fmt"
	"os"
	"testing"
)

func TestLoadRecord(t *testing.T) {
	source := "./test/aa.log"

	file, err := os.OpenFile("./record/aa.log", os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		t.Log("")
		t.Fail()
	}

	_, err = file.WriteAt([]byte(fmt.Sprintf("%d", 1024)), 0)
	if err != nil {
		t.Log("Write record failed, err:", err)
		t.Fail()
	}

	offset := LoadRecord(source)
	if offset != 1024 {
		t.Logf("offset:%d != 1024\n", offset)
		t.Fail()
	}
}
