package watermill_stomp

import (
	"sync"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/go-stomp/stomp"
	"github.com/pkg/errors"
)

type connectionWrapper struct {
	config Config

	logger watermill.LoggerAdapter

	stompConnection     *stomp.Conn
	stompConnectionLock sync.Mutex
	connected           chan struct{}

	closing chan struct{}
	closed  bool

	publishingWg  sync.WaitGroup
	subscribingWg sync.WaitGroup
}

func newConnection(
	config Config,
	logger watermill.LoggerAdapter,
) (*connectionWrapper, error) {
	if logger == nil {
		logger = watermill.NopLogger{}
	}

	pubsub := &connectionWrapper{
		config:    config,
		logger:    logger,
		closing:   make(chan struct{}),
		connected: make(chan struct{}),
	}
}

func (c *connectionWrapper) Close() error {
	if c.closed {
		return nil
	}
	c.closed = true
	close(c.closing)

	c.logger.Info("Closing STOMP Pub/Sub", nil)
	defer c.logger.Info("Closed STOMP Pub/Sub", nil)

	c.publishingWg.Wait()

	if err := c.stompConnection.Disconnect(); err != nil {
		c.logger.Error("Connection close error", err, nil)
	}

	c.subscribingWg.Wait()

	return nil
}

func (c *connectionWrapper) connect() error {
	c.stompConnectionLock.Lock()
	defer c.stompConnectionLock.Unlock()

	var connection *stomp.Conn
	var err error

	connection, err = stomp.Dial(c.config.Connection.Network, c.config.Connection.Addr)

	if err != nil {
		return errors.Wrap(err, "cannot connect to STOMP")
	}

	c.stompConnection = connection
	close(c.connected)

	c.logger.Info("Connected to STOMP", nil)

	return nil
}

func (c *connectionWrapper) Connection() *stomp.Conn {
	return c.stompConnection
}

func (c *connectionWrapper) Connected() chan struct{} {
	return c.connected
}

func (c *connectionWrapper) IsConnected() bool {
	select {
	case <-c.connected:
		return true
	default:
		return false
	}
}

func (c *connectionWrapper) handleConnectionClose() {
	for {
		c.logger.Debug("handleConnectionClose is waiting for p.connected", nil)
		<-c.connected
		c.logger.Debug("handleConnectionClose is for connection or Pub/Sub close", nil)

	}
}

func (c *connectionWrapper) reconnect() {

}
