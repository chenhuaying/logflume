package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"log"
	"os"
	"time"
)

type Harvester struct {
	Path   string
	Offset int64
	//FinishChan chan int64

	file       *os.File
	CheckEnded bool

	retryer bool
}

func (h *Harvester) Harvest(input chan bool, output chan *FileEvent) {
	h.Open()
	info, err := h.file.Stat()
	if err != nil {
		panic(fmt.Sprintf("Harvest: unexcepted error: %s", err.Error()))
	}
	defer h.file.Close()
	// TODO: safe exit
	//defer func() {
	//	h.FinishChan <- h.Offset
	//}()

	var line uint64 = 0

	// get current offset
	offset, _ := h.file.Seek(0, os.SEEK_CUR)
	// reset offset
	h.Offset = offset

	reader := bufio.NewReaderSize(h.file, harvestBufferSize)
	buffer := new(bytes.Buffer)
	var readTimeout = 3 * time.Second
	deadTime, err := time.ParseDuration(deadtime)
	if err != nil {
		log.Printf("Parse deadTime %s duration failed: %d\n", deadtime, err)
		deadTime = 60 * time.Minute
	}
	lastReadTime := time.Now()

	publishCtrl := make(chan bool)

	go Publish(output, h.Path, publishCtrl)

	var publishAble bool = true
	for {

		// check if the log can end with eof
		select {
		case <-input:
			log.Println("notify me, set EOF")
			h.CheckEnded = true
		case ctrl := <-publishCtrl:
			if publishAble != ctrl {
				publishAble = ctrl
				continue
			}
		default:
			// pass
		}

		// XXX TODO: how process failed, and set to able
		if !publishAble {
			log.Println("Remote server error, can't seed, wating...")
			if remoteAvailable {
				publishAble = true
				log.Println("Remote server Recovered")
				continue
			}
			time.Sleep(3 * time.Second)
			continue
		}

		text, bytesread, err := h.readline(reader, buffer, readTimeout)
		if err != nil {
			if err == io.EOF {
				info, _ := h.file.Stat()
				if info.Size() < h.Offset {
					log.Println("File may have been truncated, seek to beginning: ", h.Path)
					h.file.Seek(0, os.SEEK_SET)
					h.Offset = 0
				} else if age := time.Since(lastReadTime); age > deadTime && h.CheckEnded {
					log.Printf("stopping harvest of %s; last change was %v age\n", h.Path, age)
					return
				}
				continue
			} else {
				log.Printf("Unexpected state reading from %s; error: %s\n", h.Path, err)
				return
			}
		}
		lastReadTime = time.Now()
		line++
		event := &FileEvent{
			Source:   &h.Path,
			Offset:   h.Offset,
			Line:     line,
			Text:     text,
			fileinfo: &info,
			RawBytes: int64(bytesread),
		}
		h.Offset += int64(bytesread)

		output <- event
	}
}

func (h *Harvester) HarvestSync(input chan bool, output chan *FileEvent) {
	h.Open()
	info, err := h.file.Stat()
	if err != nil {
		panic(fmt.Sprintf("Harvest: unexcepted error: %s", err.Error()))
	}
	defer h.file.Close()

	var line uint64 = 0

	// get current offset
	offset, _ := h.file.Seek(0, os.SEEK_CUR)
	// reset offset
	h.Offset = offset

	reader := bufio.NewReaderSize(h.file, harvestBufferSize)
	buffer := new(bytes.Buffer)
	var readTimeout = 3 * time.Second
	deadTime, err := time.ParseDuration(deadtime)
	if err != nil {
		log.Printf("Parse deadTime %s duration failed: %d\n", deadtime, err)
		deadTime = 60 * time.Minute
	}
	lastReadTime := time.Now()

	go PublishSync(output, h.Path, h.retryer)

	for {

		log.Println("Harvester Sync loop...")
		// check if the log can end with eof
		select {
		case <-input:
			log.Println("notify me, set EOF")
			h.CheckEnded = true
		default:
			// pass
		}

		text, bytesread, err := h.readline(reader, buffer, readTimeout)
		if err != nil {
			if err == io.EOF {
				info, _ := h.file.Stat()
				if info.Size() < h.Offset {
					log.Println("File may have been truncated, seek to beginning: ", h.Path)
					h.file.Seek(0, os.SEEK_SET)
					h.Offset = 0
				} else if age := time.Since(lastReadTime); age > deadTime && h.CheckEnded {
					log.Printf("stopping harvest of %s; last change was %v age\n", h.Path, age)
					return
				}
				continue
			} else {
				log.Printf("Unexpected state reading from %s; error: %s\n", h.Path, err)
				return
			}
		}
		lastReadTime = time.Now()
		line++
		event := &FileEvent{
			Source:   &h.Path,
			Offset:   h.Offset,
			Line:     line,
			Text:     text,
			fileinfo: &info,
			RawBytes: int64(bytesread),
		}
		h.Offset += int64(bytesread)

		output <- event
	}
}

func (h *Harvester) Open() *os.File {
	for {
		var err error
		h.file, err = os.Open(h.Path)
		if err != nil {
			log.Printf("open %s err:%s\n", h.Path, err)
			time.Sleep(3 * time.Second)
		} else {
			break
		}
	}

	// NOTE: seek to break point, Offset must be set by prospector
	if h.Offset > 0 {
		h.file.Seek(h.Offset, os.SEEK_SET)
	} else if tailOnLog {
		h.file.Seek(0, os.SEEK_END)
	} else {
		h.file.Seek(0, os.SEEK_SET)
	}
	return h.file
}

func (h *Harvester) readline(reader *bufio.Reader, buffer *bytes.Buffer, eofTimeout time.Duration) (*string, int, error) {
	var isPartial bool = true
	var newLineLength int = 1
	startTime := time.Now()

	for {
		segment, err := reader.ReadBytes('\n')
		if err != nil {
			if err == io.EOF && isPartial {
				time.Sleep(1 * time.Second)
				if time.Since(startTime) > eofTimeout {
					return nil, 0, err
				}
				continue
			} else {
				return nil, 0, err
			}
		}

		if segment != nil && len(segment) > 0 {
			if segment[len(segment)-1] == '\n' {
				isPartial = false
			}
			// check if CR present
			if len(segment) > 1 && segment[len(segment)-2] == '\r' {
				newLineLength++
			}
			buffer.Write(segment)
		}

		// a full line
		if !isPartial {
			bufferSize := buffer.Len()
			str := new(string)
			// strim LF or CRLF
			*str = buffer.String()[:bufferSize-newLineLength]
			buffer.Reset()
			return str, bufferSize, nil
		}
	}

	return nil, 0, nil
}
