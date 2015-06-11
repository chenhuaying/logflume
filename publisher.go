package main

import (
	"github.com/Shopify/sarama"
	"log"
	"os"
	"time"
)

func Publish(input chan *FileEvent, source string, ctrl chan bool) {
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

	registrar := &Registrar{source: source, publishCtrl: ctrl}
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

func PublishSync(input chan *FileEvent, source string, isRetryer bool) {
	log.Println("publishSync loop")
	clientConfig := sarama.NewConfig()
	clientConfig.Producer.RequiredAcks = sarama.WaitForAll
	clientConfig.Producer.Compression = sarama.CompressionSnappy
	clientConfig.Producer.Retry.Max = 10

	brokerList := []string{"127.0.0.1:9092"}
	producer, err := sarama.NewSyncProducer(brokerList, clientConfig)
	if err != nil {
		log.Fatalln("Failed to start Sarama producer: ", err)
	}

	defer func() {
		if err := producer.Close(); err != nil {
			log.Println("Failed to shutdown producer cleanly", err)
		}
	}()

	// if retryer, use retryer backup, others use Registrar
	var recorder Recorder
	if isRetryer {
		// set to global retryer
		recorder = mainRetryer
	} else {
		registrar := &Registrar{source: source, dir: REGISTRAR_DIR}
		if _, err := registrar.OpenRecord(registrar.dir); err != nil {
			log.Println("PublishSync open record failed, error:", err)
			os.Exit(2)
		}
		recorder = registrar
	}
	for event := range input {
		log.Printf("%v, %v, %v, %v\n", *event.Source, *event.Text, event.Line, event.Offset)
		// if failed, retry send messge until succeed
		for {
			partition, offset, err := producer.SendMessage(&sarama.ProducerMessage{
				Topic: "test",
				Key:   sarama.StringEncoder("test-key"),
				Value: sarama.StringEncoder(*event.Text),
			})
			if err != nil {
				log.Printf("Failed: %s, %d, %d\n", *event.Source, event.Line, event.Offset)
				time.Sleep(3 * time.Second)
			} else {
				log.Printf("OK: %d, %d, %s\n", partition, offset, *event.Source)
				recorder.RecordOffset(event.Offset + event.RawBytes)
				break
			}
		}
	}
}
