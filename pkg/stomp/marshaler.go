package stomp

import (
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/go-stomp/stomp"
	"github.com/go-stomp/stomp/frame"
)

const DefaultMessageUUIDHeaderKey = "_watermill_message_uuid"

type Marshaler interface {
	Marshal(msg *message.Message) (stomp.Message, error)
	Unmarshal(amqpMsg *stomp.Message) (*message.Message, error)
}

type DefaultMarshaler struct {
	// Header used to store and read message UUID.
	//
	// If value is empty, DefaultMessageUUIDHeaderKey value is used.
	// If header doesn't exist, empty value is passed as message UUID.
	MessageUUIDHeaderKey string
}

func (d DefaultMarshaler) Marshal(msg *message.Message) (stomp.Message, error) {
	header := frame.Header{}

	for key, value := range msg.Metadata {
		header.Add(key, value)
	}
	header.Add(d.computeMessageUUIDHeaderKey(), msg.UUID)

	publishing := stomp.Message{
		Header: &header,
	}

	return publishing, nil
}

func (d DefaultMarshaler) UnMarshal(stompMsg *stomp.Message) (*message.Message, error) {
	msgUUIDStr, err := d.unmarshalMessageUUID(stompMsg)
	if err != nil {
		return nil, err
	}

	msg := message.NewMessage(msgUUIDStr, stompMsg.Body)
	msg.Metadata = make(message.Metadata, stompMsg.Header.Len()-1) // headers - minus uuid

	for i := 0; i < stompMsg.Header.Len(); i++ {
		key, value := stompMsg.Header.GetAt(i)

		if key == d.computeMessageUUIDHeaderKey() {
			continue
		}

		msg.Metadata[key] = value
	}

	return msg, nil
}

func (d DefaultMarshaler) unmarshalMessageUUID(stompMsg *stomp.Message) (string, error) {
	msgUUID, hasMsgUUID := stompMsg.Header.Contains(d.computeMessageUUIDHeaderKey())
	if !hasMsgUUID {
		return "", nil
	}

	return msgUUID, nil
}

func (d DefaultMarshaler) computeMessageUUIDHeaderKey() string {
	if d.MessageUUIDHeaderKey != "" {
		return d.MessageUUIDHeaderKey
	}

	return DefaultMessageUUIDHeaderKey
}
