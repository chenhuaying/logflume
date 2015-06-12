package main

type Recorder interface {
	RecordSucceed(offset int64, RawBytes int64) error
	doBackup(text string)
}
