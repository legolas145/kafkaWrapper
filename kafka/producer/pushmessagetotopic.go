package producer

import (
	"encoding/json"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func (k *KafkaProducer) PushMessageToTopic(msg interface{}, partition int32) error {

	bytes, err := json.Marshal(msg)

	if err != nil {
		k.l.Error("Kafka Producer Unable to unmarshal message : ", err)
		return err
	}
	k.l.Debugf("Kafka Producer Msg to be Pushed : %v", string(bytes))

	// Produce messages to topic (asynchronously)
	err = k.producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &k.topicToPush, Partition: partition},
		Value:          bytes,
	}, nil)

	if err != nil {
		k.l.Error("Kafka Producer Error in publishing ", err)
		return err

	}
	// Wait for message deliveries before shutting down
	k.producer.Flush(DefaultProducerFlushTimeoutSeconds * 1000)
	return nil

}
