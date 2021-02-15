package stomp

import (
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/go-stomp/stomp"
	"github.com/hashicorp/go-multierror"
	"github.com/pkg/errors"
)

type Publisher struct {
	*connectionWrapper

	config Config
	tx     *stomp.Transaction
}

func NewPublisher(config Config, logger watermill.LoggerAdapter) (*Publisher, error) {
	if err := config.ValidatePublisher(); err != nil {
		return nil, err
	}

	conn, err := newConnection(config, logger)
	if err != nil {
		return nil, err
	}

	return &Publisher{
		connectionWrapper: conn,
		config:            config,
	}, nil
}

func (p Publisher) Publish(topic string, messages ...*message.Message) (err error) {
	if p.closed {
		return errors.New("pub/sub is connection closed")
	}
	p.publishingWg.Add(1)
	defer p.publishingWg.Done()

	if !p.IsConnected() {
		return errors.New("not connected to STOMP")
	}
	return
}

func (p *Publisher) beginTransaction(conn *stomp.Conn) error {
	tx, err := conn.BeginWithError()
	if err != nil {
		return errors.Wrap(err, "cannot start transaction")
	}

	p.tx = tx
	p.logger.Trace("Transaction begun", nil)

	return nil
}

func (p *Publisher) commitTransaction(conn *stomp.Conn, err error) error {
	if err != nil {
		if rollbackErr := p.tx.AbortWithReceipt(); rollbackErr != nil {
			return multierror.Append(err, rollbackErr)
		}
	}

	return p.tx.CommitWithReceipt()
}

func (p *Publisher) publishMessage(
	destination string,
	msg *message.Message,
	conn *stomp.Conn,
	logFields watermill.LogFields,
) error {
	logFields = logFields.Add(watermill.LogFields{"message_uuid": msg.UUID})

	p.logger.Trace("Publishing message", logFields)

	stompMsg, err := p.config.Marshaler.Marshal(msg)
	if err != nil {
		return errors.Wrap(err, "cannot marshal message")
	}

	if err = conn.Send(destination, "", stompMsg.Body); err != nil {
		return errors.Wrap(err, "cannot publish msg")
	}

	p.logger.Trace("Message published", logFields)

	return nil
}
