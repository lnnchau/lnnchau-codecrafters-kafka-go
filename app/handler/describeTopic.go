package handler

import (
	"encoding/binary"
	"io"
	"log"

	"github.com/codecrafters-io/kafka-starter-go/app/common"
	"github.com/google/uuid"
)

const (
	TOPIC_ID = "00000000-0000-0000-0000-000000000000"
)

type DescribeTopicPartitionsHandler struct{}

type DescribeTopicPartitions struct {
	topics                   []Topic
	response_partition_limit int32
	cursor                   int8
	throttle_time_ms         int32
}

type Topic struct {
	TopicName string
}

func (topic_partition_object *DescribeTopicPartitions) Deserialize(body []byte) ([]byte, error) {
	topics := make([]Topic, 0)

	// Topic Array
	topic_array_length := int(body[0]) // topics array + 1
	cursor := 1
	log.Printf("Topic length from Request %d", topic_array_length)
	for i := 0; i < topic_array_length-1; i++ {
		topic_name_length := int(body[cursor])

		log.Printf("Extract topic #%d from request, length %v", i, body[cursor])

		offset_topic_name := cursor + 1
		topics = append(topics, Topic{
			TopicName: string(body[offset_topic_name : offset_topic_name+topic_name_length-1]),
		})
		cursor = offset_topic_name + topic_name_length + 1
		log.Printf("Topic length %d Topic name %s\n", topic_name_length, string(body[offset_topic_name:offset_topic_name+topic_name_length]))
	}

	// Response Partition Limit
	response_partition_limit := binary.BigEndian.Uint32(body[cursor : cursor+4])
	cursor += 4

	// TODO: handle Cursor
	cursor += 1

	topic_partition_object.topics = topics
	topic_partition_object.response_partition_limit = int32(response_partition_limit)
	topic_partition_object.throttle_time_ms = 0

	return body[cursor:], nil
}

func (topic_partition_object *DescribeTopicPartitions) Serialize() ([]byte, error) {

	output := make([]byte, 0)
	output = binary.BigEndian.AppendUint32(output, uint32(topic_partition_object.throttle_time_ms))

	output = append(output, uint8(1+len(topic_partition_object.topics)))

	log.Printf("Length of topics %d", uint8(1+len(topic_partition_object.topics)))
	for _, item := range topic_partition_object.topics {
		output = binary.BigEndian.AppendUint16(output, common.ERROR_CODE_UNKNOWN_TOPIC_OR_PARTITION)

		log.Printf("Topic name %s %d\n", item.TopicName, uint8(1+len(item.TopicName)))
		output = append(output, uint8(1+len(item.TopicName)))
		output = append(output, []byte(item.TopicName)...)

		u, err := uuid.Parse(TOPIC_ID)
		if err != nil {
			// TODO: append meaningful message to error
			return nil, err
		}
		output = append(output, u[:]...)

		output = append(output, 0) // IS INTERNAL
		output = append(output, 1) // PARTITION ARRAY

		output = append(output, []byte{0, 0, 0, 0}...) // A 4-byte integer (bitfield) representing the authorized operations for this topic.
		output = append(output, 0x00)
	}

	output = append(output, 0xff) // next cursor
	output = append(output, 0x00) // tag buffer

	log.Print("Length of body")
	log.Print(len(output))

	return output, nil
}

func (handler *DescribeTopicPartitionsHandler) validate(req common.RequestMessage) common.Error {
	return common.Error(0)
}

func (handler *DescribeTopicPartitionsHandler) Process(w io.Writer, req common.RequestMessage) {

	// TODO: redesign validate
	_ = handler.validate(req)

	header_obj := common.RequestResponseHeaderV0{CorrelationId: req.Header.CorrelationId}

	topic_partition_object := DescribeTopicPartitions{}
	_, _ = topic_partition_object.Deserialize(req.Body)

	resp_msg := common.ResponseMessage{
		Header: &header_obj,
		Body:   &topic_partition_object,
	}

	output, _ := resp_msg.Serialize()

	w.Write(output)
}
