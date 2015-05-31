package main

import (
	"github.com/Shopify/sarama"
	"log"
)

func Registrar(errorChan <-chan *sarama.ProducerError, succChan <-chan *sarama.ProducerMessage) {
	for {
		select {
		case error := <-errorChan:
			log.Println("error:", error)
		case success := <-succChan:
			log.Println("OK:", success,
				success.Metadata.(*FileEvent).Offset,
				*success.Metadata.(*FileEvent).Source)
		}
	}
}
