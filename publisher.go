package main

import (
	"github.com/Shopify/sarama"
	"log"
	"time"
)

func Publish(input chan *FileEvent, source string) {
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

	registrar := &Registrar{source: source}
	go registrar.RegistrarDo(producer.Errors(), producer.Successes())

	for event := range input {
		log.Printf("%v, %v, %v, %v\n", *event.Source, *event.Text, event.Line, event.Offset)
		producer.Input() <- &sarama.ProducerMessage{
			Topic:    "test",
			Key:      sarama.StringEncoder("test-key"),
			Value:    sarama.StringEncoder(*event.Text),
			Metadata: event,
		}
	}

}
