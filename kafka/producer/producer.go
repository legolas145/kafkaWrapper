package producer

import (
	"errors"
	"strings"

	"github.com/FenixAra/go-util/log"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/google/uuid"
)

const (
	BootStrapServersKey                = "bootstrap.servers"
	SecurityProtocolKey                = "security.protocol"
	DefaultSecurityProtocol            = "plaintext"
	DefaultProducerFlushTimeoutSeconds = 15
)

var (
	errEmptyBrokerURLs  = errors.New("The broker URLs are empty")
	errNoTopicToProduce = errors.New("No topics configured to push to")
)

type IKafkaProducer interface {
	PushMessageToTopic(msg interface{}, partition int32) error
}

type KafkaProducer struct {
	name             string
	producer         *kafka.Producer
	l                *log.Logger
	securityProtocol string
	brokerURLs       string
	topicToPush      string
	flushTimeoutSecs int
}

func NewKafkaProducer(l *log.Logger, config *KafkaProducerConfig) (IKafkaProducer, error) {
	producerWrapper := &KafkaProducer{}
	//Checking for errors
	if len(config.BrokerURLs) == 0 {
		return nil, errEmptyBrokerURLs
	}

	if config.TopicToPush == "" {
		return nil, errNoTopicToProduce
	}

	//Assigning Defaults
	if config.Name == "" {
		config.Name = uuid.New().String()
	}

	if config.SecurityProtocol == "" {
		config.SecurityProtocol = DefaultSecurityProtocol
	}
	if config.FlushTimeoutSecs == 0 {
		config.FlushTimeoutSecs = DefaultProducerFlushTimeoutSeconds
	}

	//Constructing the kafka producer
	producerWrapper.name = config.Name
	producerWrapper.brokerURLs = strings.Join(config.BrokerURLs, ",")
	producerWrapper.l = l
	producerWrapper.topicToPush = config.TopicToPush
	producerWrapper.securityProtocol = config.SecurityProtocol

	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		BootStrapServersKey: producerWrapper.brokerURLs,
		SecurityProtocolKey: producerWrapper.securityProtocol,
	})
	if err != nil {
		l.Error("Error in creating kafka producer", err)
		return nil, err
	}

	producerWrapper.producer = producer

	//Go routine for listening to delivery stats
	go func() {
		for e := range producerWrapper.producer.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					producerWrapper.l.Info("Kafka Producer Delivery failed Error:", ev.TopicPartition.Error, " Producer: ", producerWrapper.name, " Topic: ", producerWrapper.topicToPush, " Partition: ", ev.TopicPartition, " Msg ", string(ev.Value), " Key ", string(ev.Key))
				} else {
					producerWrapper.l.Info("Kafka Producer Delivery successfull Producer:", producerWrapper.name, " Topic: ", producerWrapper.topicToPush, " Partition: ", ev.TopicPartition, " Msg ", string(ev.Value), " Key ", string(ev.Key))
				}
			}
		}
	}()

	return producerWrapper, nil
}
