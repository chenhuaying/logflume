package main

import "os"

type FileEvent struct {
	Source   *string `json:"source,omitempty"`
	Offset   int64   `json:"offset,omitempty"`
	Line     uint64  `json:"line,omitempty"`
	Text     *string `json:"text,omitempty"`
	RawBytes int64   `json:"rawbytes,omitempty"`
	Fields   *map[string]string

	fileinfo *os.FileInfo
}
