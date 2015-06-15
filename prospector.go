package main

import (
	//fsnotify "gopkg.in/fsnotify.v1"
	fsnotify "github.com/howeyc/fsnotify"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strconv"
)

type Prospector struct {
	checkpoint string
	watcher    *fsnotify.Watcher
	files      map[string]*FileState
}

func OpenRecord(path string) (*os.File, error) {
	if _, err := os.Stat(path); err != nil {
		log.Printf("OpenRecord %d failed, error: %s\n", path, err)
		return nil, err
	}

	file, err := os.OpenFile(path, os.O_RDONLY, 0666)
	if err != nil {
		log.Printf("open %s failed, error:%s\n", path, err)
		return nil, err
	}
	log.Printf("Prospector open record %s ok %v\n", path, file.Name())

	return file, nil
}

func LoadRecord(source string) int64 {
	recordRoot := filepath.Join("./", "record")
	baseName := filepath.Base(source)
	recordPath := filepath.Join(recordRoot, baseName)

	file, err := OpenRecord(recordPath)
	if err != nil {
		log.Println("Source(%s), OpenRecord %s failed, use default 0 offset", source, recordPath)
		return 0
	}

	defer file.Close()

	buf := make([]byte, 1024)
	bytes, err := file.ReadAt(buf, 0)
	if err != nil {
		if err == io.EOF {
			offsetStr := string(buf)
			offsetStr = offsetStr[0:bytes]
			off, err := strconv.ParseInt(offsetStr, 10, 64)
			if err != nil {
				log.Printf("strconv offsetStr(%s) with error: %s", offsetStr, err)
				return 0
			}
			return off
		}
	}

	return 0
}

func (p *Prospector) ListDir() {
	items, err := ioutil.ReadDir(p.checkpoint)
	if err != nil {
		log.Println("ReadDir:", err)
		os.Exit(2)
	}

	for _, item := range items {
		if item.IsDir() {
			log.Printf("%s is a directory, skip", item.Name())
			continue
		}
		fileName := item.Name()
		source := filepath.Join(checkpoint, fileName)
		p.files[fileName] = &FileState{Source: &source}
	}
	log.Println(p.files)
}

func (p *Prospector) Prospect(done chan bool) {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		log.Println("NewWatch:", err)
		os.Exit(2)
	}
	defer watcher.Close()

	harvesterChans := make([]chan bool, 0)

	if pathLen := len(p.checkpoint); pathLen != 0 {
		if p.checkpoint[pathLen-1] == '/' {
			err = watcher.WatchFlags(p.checkpoint, fsnotify.FSN_CREATE)
			if err != nil {
				log.Println("Watch add: ", err)
				os.Exit(2)
			}
			p.ListDir()
		} else {
			p.files[checkpoint] = &FileState{Source: &checkpoint}
			log.Println(p.files)
		}
	}

	for _, source := range p.files {
		input := make(chan bool, 10)
		harvesterChans = append(harvesterChans, input)
		// set to last process
		var offset int64 = LoadRecord(*source.Source)
		harvester := &Harvester{Path: *source.Source, Offset: offset}
		output := make(chan *FileEvent, 16)
		go harvester.Harvest(input, output)
	}

	go func() {
		for {
			log.Println("prospect watcher loop...")
			select {
			case ev, open := <-watcher.Event:
				if !open {
					continue
				}
				if ev.IsCreate() {
					//source := filepath.Join(checkpoint, ev.Name)
					source := ev.Name
					if _, ok := p.files[filepath.Base(ev.Name)]; ok {
						// already processed
						continue
					}
					p.files[filepath.Base(ev.Name)] = &FileState{Source: &source}
					// notify Harvester new log created
					for _, notify := range harvesterChans {
						notify <- true
					}

					var offset int64 = 0
					harvester := &Harvester{Path: source, Offset: offset}
					input := make(chan bool, 10)
					harvesterChans = append(harvesterChans, input)
					output := make(chan *FileEvent, 16)
					go harvester.Harvest(input, output)
				}
				log.Println(p.files)
			case ev := <-watcher.Error:
				log.Println(ev)
			}
		}
	}()

	<-done
}
