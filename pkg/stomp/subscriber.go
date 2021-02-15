package stomp

import (
	"context"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/go-stomp/stomp"
	"github.com/pkg/errors"
)

type Subscriber struct {
	*connectionWrapper

	config Config
}

func NewSubscriber(config Config, logger watermill.LoggerAdapter) (*Subscriber, error) {
	if err := config.ValidateSubscriber(); err != nil {
		return nil, err
	}

	conn, err := newConnection(config, logger)
	if err != nil {
		return nil, err
	}

	return &Subscriber{conn, config}, nil
}

func (s Subscriber) Subscribe(ctx context.Context, topic string) (<-chan *message.Message, error) {
	if s.closed {
		return nil, errors.New("pub/sub is closed")
	}

	if !s.IsConnected() {
		return nil, errors.New("not connected to AMQP")
	}

	logFields := watermill.LogFields{"topic": topic}

	out := make(chan *message.Message)

	queueName := s.config.Queue.GenerateName(topic)
	logFields["stomp_queue_name"] = queueName

	s.subscribingWg.Add(1)
	go func(ctx context.Context) {
		defer func() {
			close(out)
			s.logger.Info("Stopped consuming from STOMP channel", logFields)
			s.subscribingWg.Done()
		}()

	ReconnectLoop:
		for {
			s.logger.Debug("Waiting for s.connected or s.closing in ReconnectLoop", logFields)

			select {
			case <-s.connected:
				s.logger.Debug("Connection established in ReconnectLoop", logFields)
				// runSubscriber blocks until connection fails or Close() is called
				s.runSubscriber(ctx, out, queueName, logFields)
			case <-s.closing:
				s.logger.Debug("Stopping ReconnectLoop (closing)", logFields)
				break ReconnectLoop
			case <-ctx.Done():
				s.logger.Debug("Stopping ReconnectLoop (ctx done)", logFields)
				break ReconnectLoop
			}

			time.Sleep(time.Millisecond * 100)
		}
	}(ctx)

	return out, nil
}

func (s *Subscriber) runSubscriber(
	ctx context.Context,
	out chan *message.Message,
	destination string,
	logFields watermill.LogFields,
) {
	conn, err := s.openSubscribeConnection(destination, logFields)
	if err != nil {
		s.logger.Error("Failed to open connection", err, logFields)
		return
	}
	defer func() {
		err := conn.Unsubscribe()
		s.logger.Error("Failed to unsubscibe connection", err, logFields)
	}()

	sub := subscription{
		out:         out,
		logFields:   logFields,
		connection:  s.stompConnection,
		destination: destination,
		logger:      s.logger,
		closing:     s.closing,
		config:      s.config,
	}

	s.logger.Info("Starting consuming from STOMP channel", logFields)

	sub.ProcessMessages(ctx)
}

func (s *Subscriber) openSubscribeConnection(
	destination string,
	logFields watermill.LogFields,
) (*stomp.Subscription, error) {
	if !s.IsConnected() {
		return nil, errors.New("not connected to STOMP")
	}

	subscription, err := s.stompConnection.Subscribe(destination, stomp.AckClientIndividual)
	if err != nil {
		return nil, errors.Wrap(err, "cannot open connection")
	}
	s.logger.Debug("Connection opened", logFields)

	return subscription, nil
}

type subscription struct {
	out         chan *message.Message
	logFields   watermill.LogFields
	connection  *stomp.Conn
	destination string

	logger  watermill.LoggerAdapter
	closing chan struct{}
	config  Config
}

func (s *subscription) ProcessMessages(ctx context.Context) {
	stompMsgs, err := s.createConsumer(s.destination, s.connection)
	if err != nil {
		s.logger.Error("Failed to start consuming messages", err, s.logFields)
		return
	}

ConsumingLoop:
	for {
		select {
		case stompMsg := <-stompMsgs:
			if err := s.processMessage(ctx, stompMsg, s.out, s.logFields); err != nil {
				s.logger.Error("Processing message failed, sending nack", err, s.logFields)

				if err := s.nackMsg(stompMsg); err != nil {
					s.logger.Error("Cannot nack message", err, s.logFields)

					// something went really wrong when we cannot nack, let's reconnect
					break ConsumingLoop
				}
			}
			continue ConsumingLoop

		case <-s.closing:
			s.logger.Info("Closing from Subscriber received", s.logFields)
			break ConsumingLoop

		case <-ctx.Done():
			s.logger.Info("Closing from ctx received", s.logFields)
			break ConsumingLoop
		}
	}
}

func (s *subscription) createConsumer(queueName string, conn *stomp.Conn) (<-chan *stomp.Message, error) {
	// AckMode must be set to AckClientIndividual - acks are managed by Watermill
	sub, err := conn.Subscribe(queueName, stomp.AckClientIndividual)
	if err != nil {
		return nil, errors.Wrap(err, "cannot consume from channel")
	}

	return sub.C, nil
}

func (s *subscription) processMessage(
	ctx context.Context,
	stompMsg *stomp.Message,
	out chan *message.Message,
	logFields watermill.LogFields,
) error {
	msg, err := s.config.Marshaler.Unmarshal(stompMsg)
	if err != nil {
		return err
	}

	ctx, cancelCtx := context.WithCancel(ctx)
	msg.SetContext(ctx)
	defer cancelCtx()

	msgLogFields := logFields.Add(watermill.LogFields{"message_uuid": msg.UUID})
	s.logger.Trace("Unmarshaled message", msgLogFields)

	select {
	case <-s.closing:
		s.logger.Info("Message not consumed, pub/sub is closing", msgLogFields)
		return s.nackMsg(stompMsg)
	case out <- msg:
		s.logger.Trace("Message sent to consumer", msgLogFields)
	}

	select {
	case <-s.closing:
		s.logger.Trace("Closing pub/sub, message discarded before ack", msgLogFields)
		return s.nackMsg(stompMsg)
	case <-msg.Acked():
		s.logger.Trace("Message Acked", msgLogFields)
		return s.connection.Ack(stompMsg)
	case <-msg.Nacked():
		s.logger.Trace("Message Nacked", msgLogFields)
		return s.nackMsg(stompMsg)
	}
}

func (s *subscription) nackMsg(stompMsg *stomp.Message) error {
	return s.connection.Nack(stompMsg)
}
