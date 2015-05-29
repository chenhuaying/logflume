package main

import (
	"log"
)

func Registrar(input chan *FileEvent) {
	for event := range input {
		log.Printf("%s, offset %d\n", *event.Source, event.Offset)
	}
}
