package main

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/Shopify/sarama"
	log "github.com/Sirupsen/logrus"
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
		log.Errorf("%s not exist, create it now", recordDir)
		err := os.MkdirAll(recordDir, 0755)
		if err != nil {
			if err != os.ErrExist {
				log.Errorf("MkdirAll %s failed, error: %s", recordDir, err)
				return nil, err
			}
		}
	}
	path := filepath.Join(recordDir, filepath.Base(r.source))
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		log.Errorf("open %s failed, error:%s", path, err)
		return nil, err
	}
	log.Infof("registrar open record %s ok %v", path, file)
	r.file = file
	return file, nil
}

func (r *Registrar) RecordOffset(offset int64) error {
	if _, err := r.file.WriteAt([]byte(fmt.Sprintf("%d", offset)), 0); err != nil {
		log.Errorf("record offset of %s failed, error: %s", r.file.Name(), err)
		return err
	}
	//log.Printf("record offset of %s, offset %d\n", r.file.Name(), offset)
	return nil
}

func (r *Registrar) RecordSucceed(offset, rawBytes int64) error {
	return r.RecordOffset(offset + rawBytes)
}

func (r *Registrar) doBackup(text string) {
}

func (r *Registrar) RegistrarDo(errorChan <-chan *sarama.ProducerError, succChan <-chan *sarama.ProducerMessage) {
	// record root dir: $work_dir/record, path: $record_root/$log_name
	recordRoot := filepath.Join("./", "record")
	if _, err := r.OpenRecord(recordRoot); err != nil {
		log.Debug("set to Silence")
		r.recordOpt = r.SilenceRecordOffset
	} else {
		log.Debug("set to file record")
		r.recordOpt = r.RecordOffset
		defer r.file.Close()
	}
	for {
		select {
		case err, ok := <-errorChan:
			if !ok {
				return
			}
			log.Error("Received Error:", err)
			fev := err.Msg.Metadata.(*FileEvent)
			r.recordOpt(err.Msg.Metadata.(*FileEvent).Offset + fev.RawBytes)
			// record to retryer
			msg := fmt.Sprint(filepath.Base(*fev.Source)+" ", *fev.Text)
			mainRetryer.doBackup(msg)
			// set remote serve failure
			remoteAvailable = false
			r.publishCtrl <- false
		case success, ok := <-succChan:
			if !ok {
				return
			}
			log.Debug("Received OK:", success.Metadata.(*FileEvent).RawBytes,
				success.Metadata.(*FileEvent).Offset,
				*success.Metadata.(*FileEvent).Source)
			fev := success.Metadata.(*FileEvent)
			//log.Printf("registrar(%s), fileEvent(%s)\n", r.file.Name(), *fev.Source)
			r.recordOpt(success.Metadata.(*FileEvent).Offset + fev.RawBytes)
			// TODO: sync file with a switch flag
			//r.file.Sync()
		}
	}
}
