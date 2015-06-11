package main

import (
	"fmt"
	"github.com/Shopify/sarama"
	"log"
	"os"
	"path/filepath"
)

type Registrar struct {
	source      string
	dir         string
	file        *os.File
	recordOpt   func(int64) error
	publishCtrl chan bool
}

const REGISTRAR_DIR string = "record"

func (r *Registrar) SilenceRecordOffset(offset int64) error {
	return nil
}

func (r *Registrar) OpenRecord(recordDir string) (*os.File, error) {
	if _, err := os.Stat(recordDir); os.IsNotExist(err) {
		log.Printf("%s not exist, create it now\n", recordDir)
		err := os.MkdirAll(recordDir, 0755)
		if err != nil {
			if err != os.ErrExist {
				log.Printf("MkdirAll %s failed, error: %s\n", recordDir, err)
				return nil, err
			}
		}
	}
	path := filepath.Join(recordDir, filepath.Base(r.source))
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		log.Printf("open %s failed, error:%s\n", path, err)
		return nil, err
	}
	log.Printf("registrar open record %s ok %v\n", path, file)
	r.file = file
	return file, nil
}

func (r *Registrar) RecordOffset(offset int64) error {
	if _, err := r.file.WriteAt([]byte(fmt.Sprintf("%d", offset)), 0); err != nil {
		log.Printf("record offset of %s failed, error: %s\n", r.file.Name(), err)
		return err
	}
	log.Printf("record offset of %s, offset %d\n", r.file.Name(), offset)
	return nil
}
func (r *Registrar) doBackup(text string) {
}

func (r *Registrar) RegistrarDo(errorChan <-chan *sarama.ProducerError, succChan <-chan *sarama.ProducerMessage) {
	// record root dir: $work_dir/record, path: $record_root/$log_name
	recordRoot := filepath.Join("./", "record")
	if _, err := r.OpenRecord(recordRoot); err != nil {
		log.Println("set to Silence")
		r.recordOpt = r.SilenceRecordOffset
	} else {
		log.Println("set to file record")
		r.recordOpt = r.RecordOffset
		defer r.file.Close()
	}
	for {
		select {
		case err := <-errorChan:
			log.Println("Received Error:", err)
			fev := err.Msg.Metadata.(*FileEvent)
			r.recordOpt(err.Msg.Metadata.(*FileEvent).Offset + fev.RawBytes)
			// record to retryer
			mainRetryer.doBackup(*fev.Text)
			r.publishCtrl <- false
		case success := <-succChan:
			log.Println("Received OK:", success.Metadata.(*FileEvent).RawBytes,
				success.Metadata.(*FileEvent).Offset,
				*success.Metadata.(*FileEvent).Source)
			fev := success.Metadata.(*FileEvent)
			log.Printf("registrar(%s), fileEvent(%s)\n", r.file.Name(), *fev.Source)
			r.recordOpt(success.Metadata.(*FileEvent).Offset + fev.RawBytes)
			// TODO: sync file with a switch flag
			r.file.Sync()
		}
	}
}
