package main

import (
	"fmt"
	"io"
	"os"
	"strconv"
	"testing"
)

func InitEnv() *Registrar {
	recordDir := "records"
	registrar := &Registrar{source: "./test/a.log"}
	_, err := registrar.OpenRecord(recordDir)
	if err != nil {
		fmt.Println("OpenRecord failed!")
		os.Exit(2)
	}
	return registrar
}

func TestOpenRecord(t *testing.T) {
	recordDir := "records"
	registrar := &Registrar{source: "./test/a.log"}
	_, err := registrar.OpenRecord(recordDir)
	if err != nil {
		t.Log("OpenRecord failed!")
		t.Fail()
	}
	t.Log("OpenRecord OK")
}

func TestRecordOffset(t *testing.T) {
	registrar := InitEnv()
	var offset int64 = 12
	if err := registrar.RecordOffset(offset); err != nil {
		t.Log("RecordOffset err", err)
		t.Fail()
	}

	//registrar.file.Sync()

	r, err := os.Open(registrar.file.Name())
	if err != nil {
		t.Log("Open record file read failed, err:", err)
		t.Fail()
	}

	buf := make([]byte, 1024)
	bytes, err := r.ReadAt(buf, 0)
	if err != nil {
		if err == io.EOF {
			offsetStr := string(buf)
			offsetStr = offsetStr[0:bytes]
			off, err := strconv.ParseInt(offsetStr, 10, 64)
			if off == offset {
				fmt.Println("OK", off, "=", offset)
			} else {
				t.Log("failed, error:", bytes, err)
				t.Fail()
			}
		} else {
			t.Log("Read record error:", err)
			t.Fail()
		}
	} else {
		t.Log("havn't eof:")
		t.Fail()
	}
	registrar.file.Close()
	os.RemoveAll("records")
}
