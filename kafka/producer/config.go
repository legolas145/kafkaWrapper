package producer

type KafkaProducerConfig struct {
	//(Optional) The name of the producer
	Name string `json:"name"`

	//(Mandatory) The topic to which this producer needs to push the data
	TopicToPush string `json:"topic_to_push"`

	//(Optional) The security protocol that needs to be used
	SecurityProtocol string `json:"security_protocol"`

	//(Mandatory) The kakfa broker URLs.
	BrokerURLs []string `json:"broker_urls"`

	//(Optional) The flush timeout Secs
	FlushTimeoutSecs int `json:"flush_timeout_seconds"`
}
