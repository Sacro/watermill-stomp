package watermill_stomp

import "github.com/ThreeDotsLabs/watermill"

type Subscriber struct {
	config Config
}

func NewSubscriber(config Config, logger watermill.LoggerAdapter) (*Subscriber, error) {
	if err := config.ValidateSubscriber(); err != nil {
		return nil, err
	}
}
