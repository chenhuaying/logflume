package main

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/Shopify/sarama"
	log "github.com/Sirupsen/logrus"
)

func Publish(input chan *FileEvent, source string, ctrl chan bool) {
	clientConfig := sarama.NewConfig()
	clientConfig.Producer.RequiredAcks = sarama.WaitForLocal
	clientConfig.Producer.Compression = sarama.CompressionSnappy
	clientConfig.Producer.Flush.Frequency = 500 * time.Millisecond
	clientConfig.Producer.Flush.Messages = 200
	clientConfig.Producer.Flush.MaxMessages = 200
	clientConfig.Producer.Flush.Bytes = 16384
	clientConfig.Producer.Return.Successes = true
	clientConfig.Producer.Partitioner = sarama.NewRoundRobinPartitioner
	clientConfig.ChannelBufferSize = kafkabuffer

	//brokerList := []string{"127.0.0.1:9092"}
	var producer sarama.AsyncProducer
	var err error
	for {
		producer, err = sarama.NewAsyncProducer(brokerList, clientConfig)
		if err != nil {
			log.Error("Publish: Failed to start Sarama producer: ", err)
			log.Info("waiting....")
			time.Sleep(1 * time.Second)
		} else {
			break
		}
	}

	defer func() {
		if err := producer.Close(); err != nil {
			log.Error("Failed to shutdown producer cleanly", err)
		}
	}()

	registrar := &Registrar{source: source, publishCtrl: ctrl}
	go registrar.RegistrarDo(producer.Errors(), producer.Successes())

	topic := kafkaTopic
	baseName := filepath.Base(source)
	if len(topicmap) > 0 {
		tmpTopic := genTopic(baseName, topicmap)
		if tmpTopic != "" {
			topic = tmpTopic
		}
	}

	key := hashKey
	for event := range input {
		log.Debugf("%v, %v, %v, %v", *event.Source, *event.Text, event.Line, event.Offset)
		key = strconv.FormatInt(event.Offset, 10)
		producer.Input() <- &sarama.ProducerMessage{
			Topic:    topic,
			Key:      sarama.StringEncoder(key),
			Value:    sarama.StringEncoder(*event.Text),
			Metadata: event,
		}
	}

}

func PublishSync(input chan *FileEvent, source string, isRetryer bool) {
	log.Debug("publishSync loop")
	clientConfig := sarama.NewConfig()
	clientConfig.Producer.RequiredAcks = sarama.WaitForAll
	clientConfig.Producer.Compression = sarama.CompressionSnappy
	clientConfig.Producer.Partitioner = sarama.NewRoundRobinPartitioner
	clientConfig.Producer.Retry.Max = 10

	topic := kafkaTopic
	key := hashKey
	if isRetryer {
		topic = retryTopic
	}
	//brokerList := []string{"127.0.0.1:9092"}
	var producer sarama.SyncProducer
	var err error
	for {
		producer, err = sarama.NewSyncProducer(brokerList, clientConfig)
		if err != nil {
			log.Error("Sync: Failed to start Sarama producer: ", err)
			log.Info("waiting...")
			time.Sleep(1 * time.Second)
		} else {
			break
		}
	}

	defer func() {
		if err := producer.Close(); err != nil {
			log.Error("Failed to shutdown producer cleanly", err)
		}
	}()

	// if retryer, use retryer backup, others use Registrar
	var recorder Recorder
	if isRetryer {
		// set to global retryer
		retryRecorder := &RetryRecorder{file: mainRetryer.vernier}
		recorder = retryRecorder
		defer retryRecorder.file.Close()
	} else {
		registrar := &Registrar{source: source, dir: REGISTRAR_DIR}
		if _, err := registrar.OpenRecord(registrar.dir); err != nil {
			log.Error("PublishSync open record failed, error:", err)
			os.Exit(2)
		}
		recorder = registrar
	}

	genMessage := func(rawMessage string) string {
		return rawMessage
	}
	// retryer message sample: 0 this is a sample message
	// 0 means, haven't retried succeed
	// 1 means have been sended
	if isRetryer {
		genMessage = func(rawMessage string) string {
			// 0|1 raw_name_of_log_file log_msg
			rawMessage = rawMessage[2:]
			idx := strings.Index(rawMessage, " ")
			return rawMessage[idx+1:]
		}
	}

	for event := range input {
		log.Debugf("%v, %v, %v, %v\n", *event.Source, *event.Text, event.Line, event.Offset)
		// if failed, retry send messge until succeed
		key = strconv.FormatInt(event.Offset, 10)
		rawMessage := *event.Text
		if isRetryer {
			if retryTopic != kafkaTopic {
				topic = retryTopic
			} else {
				baseName := getSourceName(rawMessage)
				if len(topicmap) > 0 {
					tmpTopic := genTopic(baseName, topicmap)
					if tmpTopic != "" {
						topic = tmpTopic
					}
				}
			}
		}
		message := genMessage(*event.Text)
		if rawMessage[0] == '1' {
			log.Debug("message[%s] have been seeded", rawMessage)
			continue
		}

		for {
			partition, offset, err := producer.SendMessage(&sarama.ProducerMessage{
				Topic: topic,
				Key:   sarama.StringEncoder(key),
				Value: sarama.StringEncoder(message),
			})
			if err != nil {
				log.Errorf("Failed: %s, %d, %d\n", *event.Source, event.Line, event.Offset)
				time.Sleep(3 * time.Second)
			} else {
				log.Debugf("OK: %d, %d, %s\n", partition, offset, *event.Source)
				recorder.RecordSucceed(event.Offset, event.RawBytes)
				break
			}
		}
	}
}
