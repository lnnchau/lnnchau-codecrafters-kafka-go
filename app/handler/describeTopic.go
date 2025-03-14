package handler

import (
	"encoding/binary"
	"io"
	"log"

	"github.com/codecrafters-io/kafka-starter-go/app/common"
	"github.com/codecrafters-io/kafka-starter-go/app/models"
)

type DescribeTopicPartitionsHandler struct{}

type DescribeTopicPartitions struct {
	topics                   []models.Topic
	response_partition_limit int32
	// cursor                   int8
	throttle_time_ms int32
}

func (topic_partition_object *DescribeTopicPartitions) Deserialize(body []byte) ([]byte, error) {
	topics := make([]models.Topic, 0)

	// Topic Array
	topic_array_length := int(body[0]) - 1 // topics array + 1

	log.Printf("Topic length from Request %d", topic_array_length)

	cursor := 1
	body = body[cursor:]
	for i := 0; i < topic_array_length; i++ {
		log.Printf("Extracting Topic #%d", i)
		topic := models.Topic{}
		body, _ = topic.Deserialize(body)
		topics = append(topics, topic)
	}

	// Response Partition Limit
	cursor = 0
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
		serialized_topic, _ := item.Serialize()
		output = append(output, serialized_topic...)
	}

	output = append(output, 0xff) // next cursor
	output = append(output, 0x00) // tag buffer

	log.Print("Length of body")
	log.Print(len(output))

	return output, nil
}

func (handler *DescribeTopicPartitionsHandler) validate(req models.RequestMessage) common.Error {
	return common.Error(0)
}

func (handler *DescribeTopicPartitionsHandler) Process(w io.Writer, req models.RequestMessage) {

	// TODO: redesign validate
	// log.Print("To be validate")
	_ = handler.validate(req)

	header_obj := models.RequestResponseHeaderV0{CorrelationId: req.Header.CorrelationId}

	log.Print("To be create topic_partition_object")
	topic_partition_object := DescribeTopicPartitions{}
	_, _ = topic_partition_object.Deserialize(req.Body)

	// log.Printf("Length of topic_partition_object.topics %v", len(topic_partition_object.topics))

	for i := 0; i < len(topic_partition_object.topics); i++ {
		topic_in_db, exists := models.DummyDb[topic_partition_object.topics[i].TopicName]
		if exists {
			log.Printf("exists %v", topic_partition_object.topics[i].TopicName)
			topic_partition_object.topics[i] = topic_in_db
		} else {
			log.Printf("Not exist %v", topic_partition_object.topics[i].TopicName)
			topic_partition_object.topics[i].ErrorCode = common.ERROR_CODE_UNKNOWN_TOPIC_OR_PARTITION
		}
	}

	resp_msg := models.ResponseMessage{
		Header: &header_obj,
		Body:   &topic_partition_object,
	}

	output, _ := resp_msg.Serialize()

	w.Write(output)
}
