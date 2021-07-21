package consumer

import (
	"errors"
	"runtime/debug"
	"strings"
	"sync"
	"time"

	"github.com/FenixAra/go-util/log"
)

//Default consumer group level constants
const (
	defaultSize                    = 1
	defaultHealthCheckDurationSecs = 10
	defaultSecurityProtocol        = "plaintext"
	defaultAutoOffsetReset         = "earliest"
	defaultPollIntervalSecs        = 1
)

//Validation Errors
var (
	errEmptyGroupName   = errors.New("The consumer group name is empty")
	errEmptyBrokerURLs  = errors.New("The broker URLs are empty")
	errNoTopicsToListen = errors.New("No topics configured to listen")
)

type IConsumerGroup interface {
	CreateandInitiateConsumerGroups(ConsumerGroupConfigs []KafkaConsumerGroupConfig, l *log.Logger, callBack ICallback) error
}

type ConsumerGroup struct {
	name                    string
	healthCheckDurationSecs int
	restartStoppedConsumers bool
	size                    int
	consumers               []*Consumer
	l                       *log.Logger
	brokerURLs              string
	autoOffsetReset         string
	topicsToListen          []string
	securityProtocol        string
	pollIntervalSecs        int
	CallBack                ICallback
	lck                     *sync.Mutex
}

//Performs validations checks and creates a consumer group object from consumer group config
func createConsumerGroupObjectFromConfig(consumerGroupConfig KafkaConsumerGroupConfig, l *log.Logger, callBack ICallback) (*ConsumerGroup, error) {
	group := &ConsumerGroup{}
	//Checking for errors
	if len(consumerGroupConfig.BrokerURLs) == 0 {
		return nil, errEmptyBrokerURLs
	}

	if consumerGroupConfig.Name == "" {
		return nil, errEmptyGroupName
	}

	if consumerGroupConfig.TopicToListen == "" {
		return nil, errNoTopicsToListen
	}

	//Assigning defaults

	if consumerGroupConfig.HealthCheckDurationSecs == 0 {
		consumerGroupConfig.HealthCheckDurationSecs = defaultHealthCheckDurationSecs
	}
	if consumerGroupConfig.Size == 0 {
		consumerGroupConfig.Size = defaultSize
	}
	if consumerGroupConfig.AutoOffSetReset == "" {
		consumerGroupConfig.AutoOffSetReset = defaultAutoOffsetReset
	}
	if consumerGroupConfig.SecurityProtocol == "" {
		consumerGroupConfig.SecurityProtocol = defaultSecurityProtocol
	}
	if consumerGroupConfig.PollIntervalSecs == 0 {
		consumerGroupConfig.PollIntervalSecs = defaultPollIntervalSecs
	}

	//Cobstructing the consumer group object
	group.brokerURLs = strings.Join(consumerGroupConfig.BrokerURLs, ",")
	group.name = consumerGroupConfig.Name
	group.topicsToListen = append(group.topicsToListen, consumerGroupConfig.TopicToListen)

	group.healthCheckDurationSecs = consumerGroupConfig.HealthCheckDurationSecs
	group.size = consumerGroupConfig.Size
	group.autoOffsetReset = consumerGroupConfig.AutoOffSetReset
	group.securityProtocol = consumerGroupConfig.SecurityProtocol
	group.pollIntervalSecs = consumerGroupConfig.PollIntervalSecs

	group.restartStoppedConsumers = consumerGroupConfig.RestartStoppedConsumers
	group.l = l
	group.CallBack = callBack
	group.lck = &sync.Mutex{}

	return group, nil
}

func newConsumerGroups(ConsumerGroupConfigs []KafkaConsumerGroupConfig, l *log.Logger, callBack ICallback) ([]*ConsumerGroup, error) {
	groups := []*ConsumerGroup{}
	for _, val := range ConsumerGroupConfigs {
		//Creating Consumer Group Object from config
		group, err := createConsumerGroupObjectFromConfig(val, l, callBack)
		if err != nil {
			l.Error("Error in creating consumer group object from consumer group config", err)
			return nil, err
		}
		//Create the consumers for each group
		for i := 1; i <= group.size; i++ {
			consumer, err := newConsumer(i, group)
			if err != nil {
				l.Error("Error in creating consumer ", i, " On consumer group ", group.name)
				return nil, err
			}
			group.consumers = append(group.consumers, consumer)
		}

		groups = append(groups, group)
	}
	return groups, nil
}

//Starting the consumer go routines pertaining to a kafka consumer group
func startConsumersInGroup(consumerGroup *ConsumerGroup) {
	defer func() {
		if p := recover(); p != nil {
			consumerGroup.l.Info("Consumer group ", consumerGroup.name, " recovering from panic")
			consumerGroup.l.Errorf("Recovered from panic: %v\n %s", p, string(debug.Stack()))
		}
		consumerGroup.l.Info("Consumer group ", consumerGroup.name, " restarting ")
		go startConsumersInGroup(consumerGroup)
	}()

	for _, consumer := range consumerGroup.consumers {
		consumerGroup.l.Info("Starting consumer ", consumer.id, " of group ", consumerGroup.name)
		go consumer.read()
	}

	//Health check polling for consumers in the group
	for {
		time.Sleep(time.Duration(consumerGroup.healthCheckDurationSecs) * time.Second)
		for i, consumer := range consumerGroup.consumers {

			consumerGroup.lck.Lock()
			isConsumerExited := consumer.consumerExited
			consumerGroup.lck.Unlock()

			if isConsumerExited {
				consumerGroup.l.Error("Consumer ", consumer.id, " is unhealthy in group ", consumerGroup.name)
				if consumerGroup.restartStoppedConsumers {
					consumerGroup.l.Info("Restarting Consumer ", consumer.id, " in the group ", consumerGroup.name)
					newconsumer, err := newConsumer(consumer.id, consumerGroup)
					if err != nil {
						consumerGroup.l.Error("Error in restarting consumer ", consumer.id, " in the group ", consumerGroup.name, " Skipping ")
						continue
					}

					consumerGroup.consumers[i] = newconsumer
					go consumerGroup.consumers[i].read()
					//Assuming that go's garbage collector automatically frees the old consumer pointer that has stopped

				}
			} else {
				consumerGroup.l.Info("Consumer ", consumer.id, " is healthy in group ", consumerGroup.name)
			}

		}
	}
}

//Creates the consumer groups as per the configs provided and starts the consumers go routines associated with each group
func CreateandInitiateConsumerGroups(ConsumerGroupConfigs []KafkaConsumerGroupConfig, l *log.Logger, callBack ICallback) error {
	groups, err := newConsumerGroups(ConsumerGroupConfigs, l, callBack)
	if err != nil {
		l.Error("Error in initialising consumer groups", err)
		return err
	}

	for _, group := range groups {
		go startConsumersInGroup(group)
	}
	return nil
}
