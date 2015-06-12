package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"
)

const FORMAT_TIME_DAY string = "20060102"
const FORMAT_TIME_HOUR string = "2006010215"

const RETRY_FLAG_UNDO string = "0"
const RETRY_FLAG_DONE string = "1"

type Retryer struct {
	mu       sync.Mutex
	file     *os.File
	fileName string
	prefix   string //0: haven't processed, 1: retry ok
	suffix   string

	vernier *os.File // used for succeed record

	lock sync.Mutex
}

func NewRetryer(path string) (*Retryer, error) {
	dir := filepath.Dir(path)
	err := os.MkdirAll(dir, 0755)
	// if dir already exist, haven't an error!
	if err != nil {
		if err != os.ErrExist {
			log.Printf("MkdirAll %s failed, error: %s\n", dir, err)
			return nil, err
		}
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		log.Printf("Open retryer path[%s] failed, err: %s\n", path, err)
		return nil, err
	}
	return &Retryer{fileName: path, file: file, prefix: RETRY_FLAG_UNDO, suffix: genDayTime(time.Now())}, nil
}

func genDayTime(t time.Time) string {
	return t.Format(FORMAT_TIME_DAY)
}

func (r *Retryer) rotate() error {
	r.lock.Lock()
	defer r.lock.Unlock()

	var suffix string
	suffix = genDayTime(time.Now())

	if suffix != r.suffix {
		err := r.doRotate(suffix)
		if err != nil {
			return nil
		}
	}

	return nil
}

func (r *Retryer) doRotate(suffix string) error {
	lastFileName := r.fileName + "." + r.suffix
	err := os.Rename(r.fileName, lastFileName)
	if err != nil {
		log.Printf("doRotate of %s to %s failed, err:\n", r.fileName, lastFileName, err)
		return err
	}

	r.file.Close()

	file, err := os.OpenFile(r.fileName, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		log.Printf("Open retryer path[%s] failed, err: %s\n", r.fileName, err)
		return err
	}

	r.file = file
	r.suffix = suffix

	return nil
}

func (r *Retryer) doBackup(text string) {
	log.Println("retryer do backup")
	r.mu.Lock()
	defer r.mu.Unlock()

	err := r.rotate()
	if err != nil {
		log.Printf("Retryer backup %s failed when rotate, err: %s\n", text, err)
		return
	}

	s := fmt.Sprintln(r.prefix, text)
	_, err = r.file.WriteString(s)
	if err != nil {
		log.Printf("Retryer backup %s failed, err: %s\n", text, err)
	}
}

func (r *Retryer) RecordSucceed(offset int64, RawBytes int64) error {
	_, err := r.vernier.WriteAt([]byte(RETRY_FLAG_DONE), offset)
	remoteAvailable = true
	return err
}

func (r *Retryer) doRetry() {
	source := r.fileName

	_, err := os.Stat(filepath.Dir(source))
	if os.IsNotExist(err) {
		if err := os.MkdirAll(filepath.Dir(source), 0755); err != nil {
			log.Println("Retry %s Error: %s, fatal\n", source, err)
			os.Exit(2)
		}
	}
	log.Println("doRetry begin.....")

	var offset int64 = 0
	input := make(chan bool, 10)
	output := make(chan *FileEvent, 16)
	h := &Harvester{Path: source, Offset: offset, retryer: true}
	go h.HarvestSync(input, output)

	info, err := os.Stat(source)
	if err != nil {
		log.Print("Just open and get Stat of %s failed, error: %s\n", source, err)
		os.Exit(2)
	}

	file, err := os.OpenFile(r.fileName, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Printf("Open retryer path[%s] failed, err: %s\n", r.fileName, err)
		os.Exit(2)
	}
	defer file.Close()

	r.vernier = file

	stat := info.Sys().(*syscall.Stat_t)

	for {
		log.Println("Retryer, check looping...")
		newInfo, err := os.Stat(source)
		if err != nil {
			log.Print("Get Stat of %s failed, error: %s\n", source, err)
			continue
		}
		newStat := newInfo.Sys().(*syscall.Stat_t)
		if stat.Ino != newStat.Ino {
			input <- true
		}
		time.Sleep(3 * time.Second)
	}
}
