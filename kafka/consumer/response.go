package consumer

type KafkaWrapperConsumerResponse struct {
	Message       string `json:"message"`
	Event         string `json:"event"`
	ConsumerGroup string `json:"consumer_group"`
	Topic         string `json:"topic"`
}
