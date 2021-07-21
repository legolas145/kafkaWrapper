package consumer

import (
	"fmt"
	"runtime/debug"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

//Consumer level constants and defaults
const (
	bootStrapServersKey = "bootstrap.servers"
	securityProtocolKey = "security.protocol"
	autoOffSetResetKey  = "auto.offset.reset"
	groupIdKey          = "group.id"
)

//Callback Events
const (
	CallBackEventMsgReceived    = "Message Received"
	CallBackEventConsumerExited = "Consumer Exited"
	CallBackEventKafkaError     = "Received Error From Kafka"
)

//Call back Interface to process the messages and errors obtained from kafka
type ICallback interface {
	Process(res *KafkaWrapperConsumerResponse)
}

// Consumer is a Kafka-Consumer that connects to a Kafka Broker
// And Reads Messages from Topics
type Consumer struct {
	id             int
	consumer       *kafka.Consumer
	consumerExited bool
	groupConfig    *ConsumerGroup
}

//Creates a new kafka consumer
func newKafkaConsumer(brokerURLs string, groupname string, autoOffsetReset string, securityProtocol string, topics []string) (*kafka.Consumer, error) {

	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		bootStrapServersKey: brokerURLs,
		groupIdKey:          groupname,
		autoOffSetResetKey:  autoOffsetReset,
		securityProtocolKey: securityProtocol,
	})

	if err != nil {
		return nil, err
	}

	err = consumer.SubscribeTopics(topics, nil)
	if err != nil {
		return nil, err
	}

	return consumer, nil

}

// NewConsumer returns an initialised Kafka Consumer
func newConsumer(id int, groupConfig *ConsumerGroup) (*Consumer, error) {

	consumer, err := newKafkaConsumer(groupConfig.brokerURLs, groupConfig.name, groupConfig.autoOffsetReset, groupConfig.securityProtocol, groupConfig.topicsToListen)

	if err != nil {
		groupConfig.l.Error("Error in creating kafka consumer in group", groupConfig.name, err)
		return nil, err
	}

	return &Consumer{
		id:             id,
		consumer:       consumer,
		consumerExited: false,
		groupConfig:    groupConfig,
	}, nil
}

//Read from kafka
func (c *Consumer) read() {

	defer func() {

		if p := recover(); p != nil {
			c.groupConfig.l.Info("Consumer ", c.id, " of group ", c.groupConfig.name, " recovering from panic")
			c.groupConfig.l.Errorf("Recovered from panic: %v\n %s", p, string(debug.Stack()))
		}

		c.groupConfig.l.Info("Consumer ", c.id, " of group ", c.groupConfig.name, " is exiting ")

		res := &KafkaWrapperConsumerResponse{}
		res.Message = fmt.Sprintf("Consumer %d is exiting in group %s", c.id, c.groupConfig.name)
		res.Event = CallBackEventConsumerExited
		res.ConsumerGroup = c.groupConfig.name

		c.groupConfig.CallBack.Process(res)
		c.consumer.Close()
		//Consumer Exited variable is shared between the consumer and consumer group.Uses locks to prevent data races
		c.groupConfig.lck.Lock()
		c.consumerExited = true
		c.groupConfig.lck.Unlock()

	}()

	for {

		//Polling Kafka for messages to consume
		ev := c.consumer.Poll(c.groupConfig.pollIntervalSecs * 1000)

		// Continue on Empty Event
		if ev == nil {
			continue
		}

		// Handle Kafka Events Returned By Poll
		switch e := ev.(type) {
		// Message from Kafka
		case *kafka.Message:
			message := string(e.Value)
			c.groupConfig.l.Infof("Message Received By Consumer %d on %s: %s", c.id, *e.TopicPartition.Topic, message)

			if e.Headers != nil {
				c.groupConfig.l.Info("Message Headers: %v", e.Headers)
			}

			// Validate Empty Message
			// If Empty, dont attempt to process it
			if len(message) == 0 {
				c.groupConfig.l.Info("Empty Message Received By Consumer %d ", c.id)
				break
			}

			// Process Message
			res := &KafkaWrapperConsumerResponse{}
			res.Message = message
			res.Event = CallBackEventMsgReceived
			res.ConsumerGroup = c.groupConfig.name
			res.Topic = *e.TopicPartition.Topic

			c.groupConfig.CallBack.Process(res)

		case kafka.PartitionEOF:
			// Reached EOF of Partition
			c.groupConfig.l.Info("Consumer ", c.id, " reached partion EOF")

		case kafka.Error:
			// Error from Kafka
			c.groupConfig.l.Error("Consumer ", c.id, " has encountered error while processing from kafka Error", e.Error(), " Error code ", e.Code().String())

			res := &KafkaWrapperConsumerResponse{}
			res.Message = fmt.Sprint("Consumer ", c.id, " has encountered error while processing from kafka Error", e.Error(), " Error code ", e.Code().String())
			res.Event = CallBackEventKafkaError
			res.ConsumerGroup = c.groupConfig.name
			c.groupConfig.CallBack.Process(res)

		default:
			c.groupConfig.l.Info("Consumer ", c.id, " recevied other event ", e.String())
		}

	}
}
