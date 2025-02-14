package handler

import (
	"encoding/binary"
	"fmt"
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
	tag_buffer               int8
	throttle_time_ms         int32
}

type Topic struct {
	TopicName string
	TagBuffer byte
}

func (handler *DescribeTopicPartitionsHandler) createDescribeTopicPartitionsObject(req common.RequestMessage) *DescribeTopicPartitions {
	body := req.Body

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
			TagBuffer: body[offset_topic_name+topic_name_length],
		})
		cursor = offset_topic_name + topic_name_length + 1
		log.Printf("Topic length %d Topic name %s\n", topic_name_length, string(body[offset_topic_name:offset_topic_name+topic_name_length]))
	}

	// Response Partition Limit
	response_partition_limit := binary.BigEndian.Uint32(body[cursor : cursor+4])
	cursor += 4

	// TODO: handle Cursor
	cursor += 1

	// TagBuffer
	tag_buffer := body[cursor]

	return &DescribeTopicPartitions{
		topics:                   topics,
		response_partition_limit: int32(response_partition_limit),
		tag_buffer:               int8(tag_buffer),
		throttle_time_ms:         0,
	}
}

func (handler *DescribeTopicPartitionsHandler) validate(req common.RequestMessage) common.Error {
	return common.Error(0)
}

func (handler *DescribeTopicPartitionsHandler) Process(w io.Writer, req common.RequestMessage) {

	// TODO: redesign validate
	_ = handler.validate(req)

	topic_partition_object := handler.createDescribeTopicPartitionsObject(req)

	response := make([]byte, 0)

	// Header
	log.Printf("CorrelationID %d", req.Header.CorrelationId)
	response = binary.BigEndian.AppendUint32(response, uint32(req.Header.CorrelationId))
	response = append(response, 0x00) // tag buffer

	// Body
	response = binary.BigEndian.AppendUint32(response, uint32(topic_partition_object.throttle_time_ms))

	response = append(response, uint8(1+len(topic_partition_object.topics)))

	log.Printf("Length of topics %d", uint8(1+len(topic_partition_object.topics)))
	for _, item := range topic_partition_object.topics {
		response = binary.BigEndian.AppendUint16(response, common.ERROR_CODE_UNKNOWN_TOPIC_OR_PARTITION)

		log.Printf("Topic name %s %d\n", item.TopicName, uint8(1+len(item.TopicName)))
		response = append(response, uint8(1+len(item.TopicName)))
		response = append(response, []byte(item.TopicName)...)

		u, err := uuid.Parse(TOPIC_ID)
		if err != nil {
			fmt.Println(w, "Error while parsing TOPIC ID")
		}
		response = append(response, u[:]...)

		response = append(response, 0) // IS INTERNAL
		response = append(response, 1) // PARTITION ARRAY

		response = append(response, []byte{0, 0, 0, 0}...) // A 4-byte integer (bitfield) representing the authorized operations for this topic.
		response = append(response, 0x00)
	}

	response = append(response, 0xff) // next cursor
	response = append(response, 0x00) // tag buffer

	log.Print("Length of body")
	log.Print(len(response))

	output := make([]byte, 0)
	output = binary.BigEndian.AppendUint32(output, uint32(len(response)))
	output = append(output, response...)

	w.Write(output)
}
