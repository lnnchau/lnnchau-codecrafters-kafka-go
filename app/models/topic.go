package models

import (
	"encoding/binary"
	"log"

	"github.com/google/uuid"
)

const (
	TOPIC_ID = "00000000-0000-0000-0000-000000000000"
)

type Topic struct {
	TopicName string
	TopicId   string
}

func (topic *Topic) Deserialize(body []byte) ([]byte, error) {
	cursor := 0
	topic_name_length := int(body[cursor])

	log.Printf("Extract topic from request, length %v", body[cursor])

	offset_topic_name := cursor + 1

	topic.TopicName = string(body[offset_topic_name : offset_topic_name+topic_name_length-1])
	topic.TopicId = TOPIC_ID
	cursor = offset_topic_name + topic_name_length + 1
	log.Printf("Topic length %d Topic name %s\n", topic_name_length, string(body[offset_topic_name:offset_topic_name+topic_name_length]))

	return body[cursor:], nil
}

func (topic *Topic) Serialize() ([]byte, error) {
	output := make([]byte, 0)
	output = binary.BigEndian.AppendUint16(output, 0) // common.ERROR_CODE_UNKNOWN_TOPIC_OR_PARTITION)

	log.Printf("Topic name %s %d\n", topic.TopicName, uint8(1+len(topic.TopicName)))
	output = append(output, uint8(1+len(topic.TopicName)))
	output = append(output, []byte(topic.TopicName)...)

	u, err := uuid.Parse(topic.TopicId)
	if err != nil {
		// TODO: append meaningful message to error
		return nil, err
	}
	output = append(output, u[:]...)

	output = append(output, 0) // IS INTERNAL
	output = append(output, 1) // PARTITION ARRAY

	output = append(output, []byte{0, 0, 0, 0}...) // A 4-byte integer (bitfield) representing the authorized operations for this topic.
	output = append(output, 0x00)

	return output, nil
}
