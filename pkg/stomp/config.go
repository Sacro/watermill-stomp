package stomp

type Config struct {
	Connection ConnectionConfig

	Marshaler Marshaler

	Queue QueueConfig
}

func (c Config) validate() error {
	var err error

	return err
}

func (c Config) ValidatePublisher() error {
	err := c.validate()

	return err
}

func (c Config) ValidateSubscriber() error {
	err := c.validate()

	return err
}

type ConnectionConfig struct {
	Network string
	Addr    string
}

// QueueNameGenerator generates QueueName based on the topic.
type QueueNameGenerator func(topic string) string

// GenerateQueueNameTopicName generates queueName equal to the topic.
func GenerateQueueNameTopicName(topic string) string {
	return topic
}

// GenerateQueueNameConstant generate queue name equal to queueName.
func GenerateQueueNameConstant(queueName string) QueueNameGenerator {
	return func(topic string) string {
		return queueName
	}
}

// GenerateQueueNameTopicNameWithSuffix generates queue name equal to:
// 	topic + "_" + suffix
func GenerateQueueNameTopicNameWithSuffix(suffix string) QueueNameGenerator {
	return func(topic string) string {
		return topic + "_" + suffix
	}
}

type QueueConfig struct {
	// GenerateRoutingKey is generated based on the topic provided for Subscribe.
	GenerateName QueueNameGenerator
}
