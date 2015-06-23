package main

import (
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"time"

	log "github.com/Sirupsen/logrus"
	//fsnotify "gopkg.in/fsnotify.v1"
	fsnotify "github.com/howeyc/fsnotify"
)

type Prospector struct {
	checkpoint string
	watcher    *fsnotify.Watcher
	files      map[string]*FileState
}

func OpenRecord(path string) (*os.File, error) {
	if _, err := os.Stat(path); err != nil {
		log.Errorf("OpenRecord %d failed, error: %s", path, err)
		return nil, err
	}

	file, err := os.OpenFile(path, os.O_RDONLY, 0666)
	if err != nil {
		log.Errorf("open %s failed, error:%s", path, err)
		return nil, err
	}
	log.Errorf("Prospector open record %s ok %v", path, file.Name())

	return file, nil
}

func LoadRecord(source string) int64 {
	recordRoot := filepath.Join("./", "record")
	baseName := filepath.Base(source)
	recordPath := filepath.Join(recordRoot, baseName)

	file, err := OpenRecord(recordPath)
	if err != nil {
		log.Errorf("Source(%s), OpenRecord %s failed, use default 0 offset", source, recordPath)
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
				log.Errorf("strconv offsetStr(%s) with error: %s", offsetStr, err)
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
		log.Error("ReadDir:", err)
		os.Exit(2)
	}

	for _, item := range items {
		if item.IsDir() {
			log.Info("%s is a directory, skip", item.Name())
			continue
		}
		mtime := item.ModTime()
		startTime, err := time.ParseDuration(starttime)
		if err != nil {
			startTime = 1 * time.Hour
		}
		if age := time.Since(mtime); age >= startTime {
			continue
		}
		fileName := item.Name()
		source := filepath.Join(checkpoint, fileName)
		p.files[fileName] = &FileState{Source: &source}
	}
	log.Debug(p.files)
}

func (p *Prospector) Prospect(done chan bool) {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		log.Error("NewWatch:", err)
		os.Exit(2)
	}
	defer watcher.Close()

	harvesterChans := make([]chan bool, 0)

	if pathLen := len(p.checkpoint); pathLen != 0 {
		if p.checkpoint[pathLen-1] == '/' {
			err = watcher.WatchFlags(p.checkpoint, fsnotify.FSN_CREATE)
			if err != nil {
				log.Error("Watch add: ", err)
				os.Exit(2)
			}
			p.ListDir()
		} else {
			p.files[checkpoint] = &FileState{Source: &checkpoint}
			log.Debug(p.files)
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
			log.Debug("prospect watcher loop...")
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
						select {
						case notify <- true:
							//do nothing
						default:
							//warnning!
							log.Warn("notify channel full, may have errors")
						}
					}

					harvesterChans = harvesterChans[len(harvesterChans):]

					var offset int64 = 0
					harvester := &Harvester{Path: source, Offset: offset}
					input := make(chan bool, 10)
					harvesterChans = append(harvesterChans, input)
					output := make(chan *FileEvent, 16)
					go harvester.Harvest(input, output)
				}
				log.Debug(p.files)
			case ev := <-watcher.Error:
				log.Error(ev)
			}
		}
	}()

	<-done
}
