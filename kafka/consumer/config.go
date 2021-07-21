package consumer

type KafkaConsumerGroupConfig struct {
	//(Mandatory) The name of the consumer group that the consumers needs to listen to.
	Name string `json:"name"`

	//(Mandatory) The kafka topics that the consumers in the group needs to listen to.
	TopicToListen string `json:"topic_to_listen"`

	//(Optional) The number of consumers go routines in this group.
	Size int `json:"size"`

	//(Mandatory) The kafka broker URLs that the consumers need to connect to.
	BrokerURLs []string `json:"broker_urls"`

	//(Optional) The time duration for whcih each of the consumer polls kafka to receive messages.
	PollIntervalSecs int `json:"poll_interval_seconds"`

	//(Optional) The time duration for which the consumer group go routine periodically checks whether the consumer go routine is healthy or not
	HealthCheckDurationSecs int `json:"consumer_health_check_duration_secs"`

	//(Optional) Flag to indicate whether exited consumer go routines needs to be restarted
	RestartStoppedConsumers bool `json:"restart_stopped_consumers"`

	//(Optional) The auto offset reset
	AutoOffSetReset string `json:"auto_offset_reset"`

	//(Optional) The security protocol that needs to be used by consumer when connecting to Kafka
	SecurityProtocol string `json:"security_protocol"`
}
