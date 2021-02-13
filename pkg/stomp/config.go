package watermill_stomp

type Config struct {
	Connection ConnectionConfig
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
