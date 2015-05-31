package main

import (
	fsnotify "gopkg.in/fsnotify.v1"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
)

type Prospector struct {
	checkpoint string
	watcher    *fsnotify.Watcher
	files      map[string]*FileState
}

func (p *Prospector) ListDir() {
	items, err := ioutil.ReadDir(p.checkpoint)
	if err != nil {
		log.Println(err)
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

func (p *Prospector) Prospect(done chan bool, output chan *FileEvent) {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		log.Println(err)
		os.Exit(2)
	}
	defer watcher.Close()

	harvesterChans := make([]chan bool, 0)

	if pathLen := len(p.checkpoint); pathLen != 0 {
		if p.checkpoint[pathLen-1] == '/' {
			err = watcher.Add(p.checkpoint)
			if err != nil {
				log.Println(err)
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
		// TOTO: set to last process
		var offset int64 = 0
		harvester := &Harvester{Path: *source.Source, Offset: offset}
		go harvester.Harvest(input, output)
	}

	go func() {
		for {
			log.Println("prospect watcher loop...")
			select {
			case ev, open := <-watcher.Events:
				if !open {
					continue
				}
				if ev.Op&fsnotify.Create == fsnotify.Create {
					//source := filepath.Join(checkpoint, ev.Name)
					source := ev.Name
					p.files[filepath.Base(ev.Name)] = &FileState{Source: &source}
					// notify Harvester new log created
					for _, notify := range harvesterChans {
						notify <- true
					}

					var offset int64 = 0
					harvester := &Harvester{Path: source, Offset: offset}
					input := make(chan bool, 10)
					harvesterChans = append(harvesterChans, input)
					go harvester.Harvest(input, output)
				}
				log.Println(p.files)
			case ev := <-watcher.Errors:
				log.Println(ev)
			}
		}
	}()

	<-done
}
