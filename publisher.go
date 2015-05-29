package main

import (
	_ "github.com/Shopify/sarama"
	"log"
)

func Publish(input chan *FileEvent, registrar chan *FileEvent) {
	log.Println("publish loop")
	for event := range input {
		log.Println(*event.Text)
		registrar <- event
	}
}
