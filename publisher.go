package main

import (
	"github.com/Shopify/sarama"
	"log"
	"time"
)

func Publish(input chan *FileEvent, registrar chan *FileEvent) {
	log.Println("publish loop")

	clientConfig := sarama.NewConfig()
	clientConfig.Producer.RequiredAcks = sarama.WaitForLocal
	clientConfig.Producer.Compression = sarama.CompressionSnappy
	clientConfig.Producer.Flush.Frequency = 500 * time.Millisecond
	clientConfig.Producer.Return.Successes = true

	brokerList := []string{"127.0.0.1:9092"}
	producer, err := sarama.NewAsyncProducer(brokerList, clientConfig)
	if err != nil {
		log.Fatalln("Failed to start Sarama producer: ", err)
	}

	defer func() {
		if err := producer.Close(); err != nil {
			log.Println("Failed to shutdown producer cleanly", err)
		}
	}()

	go Registrar(producer.Errors(), producer.Successes())
	//go func() {
	//	for {
	//		select {
	//		case error := <-producer.Errors():
	//			log.Println("error:", error)
	//		case success := <-producer.Successes():
	//			log.Println("OK:", success,
	//				success.Metadata.(*FileEvent).Offset,
	//				*success.Metadata.(*FileEvent).Source)
	//		}
	//	}
	//}()

	for event := range input {
		log.Printf("%v, %v, %v, %v\n", *event.Source, *event.Text, event.Line, event.Offset)
		producer.Input() <- &sarama.ProducerMessage{
			Topic:    "test",
			Key:      sarama.StringEncoder("test-key"),
			Value:    sarama.StringEncoder(*event.Text),
			Metadata: event,
		}
		//registrar <- event
	}

}
