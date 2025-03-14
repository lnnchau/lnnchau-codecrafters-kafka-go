package models

import (
	"encoding/binary"
	"log"

	"github.com/codecrafters-io/kafka-starter-go/app/common"
	"github.com/google/uuid"
)

const (
	TOPIC_ID = "00000000-0000-0000-0000-000000000000"
)

var DummyDb map[string]Topic

type Topic struct {
	TopicName  string
	TopicId    string
	ErrorCode  common.Error
	Partitions []Partition
}

func (topic *Topic) Deserialize(body []byte) ([]byte, error) {
	cursor := 0
	topic_name_length := int(body[cursor]) - 1

	log.Printf("Extract topic from request, length %v", body[cursor])

	offset_topic_name := cursor + 1

	topic.TopicName = string(body[offset_topic_name : offset_topic_name+topic_name_length])
	topic.TopicId = TOPIC_ID
	cursor = offset_topic_name + topic_name_length + 1
	log.Printf("Topic length %d Topic name %s\n", topic_name_length, string(body[offset_topic_name:offset_topic_name+topic_name_length]))

	return body[cursor:], nil
}

func (topic *Topic) Serialize() ([]byte, error) {
	output := make([]byte, 0)

	output = binary.BigEndian.AppendUint16(output, uint16(topic.ErrorCode)) // Error Code

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
	// PARTITION ARRAY
	output = append(output, uint8(len(topic.Partitions)+1))
	var partition Partition
	var partition_bytes []byte
	for i := 0; i < len(topic.Partitions); i++ {
		partition = topic.Partitions[i]
		partition_bytes, _ = partition.Serialize()
		output = append(output, partition_bytes...)
	}

	output = append(output, []byte{0, 0, 0, 0}...) // A 4-byte integer (bitfield) representing the authorized operations for this topic.
	output = append(output, 0x00)

	return output, nil
}
