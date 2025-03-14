package utils

import (
	"encoding/binary"
	"io"
	"log"
	"os"

	"github.com/codecrafters-io/kafka-starter-go/app/models"
	"github.com/google/uuid"
	"google.golang.org/protobuf/encoding/protowire"
)

func ParseRequest(request_bytes []byte) models.RequestMessage {
	message_size := binary.BigEndian.Uint32(request_bytes[0:4])

	header := models.RequestHeaderV2{}
	body_bytes, _ := header.Deserialize(request_bytes[4:])
	log.Printf("body bytes %v", string(body_bytes))

	log.Printf("header %v", header)
	return models.RequestMessage{
		MessageSize: int32(message_size),
		Header:      header,
		Body:        body_bytes,
	}
}

func ParsePartition(b []byte) (models.Partition, []byte) {
	// - Length 2
	// - Attributes 1
	// - Timestamp Delta 1
	// - Offset Delta 1
	b = b[5:]

	// Key Length 1
	key_length := protowire.DecodeZigZag(uint64(b[0]))
	log.Printf("partition: Key length %d", int8(key_length))
	b = b[1:]
	// Key..
	if key_length > 0 {
		b = b[key_length:]
	}
	// Value Length: 2
	b = b[2:]

	log.Printf("partition: Frame version %v", b[0])
	log.Printf("partition: Type %v", b[1])
	log.Printf("partition: Version %v", b[2])

	b = b[3:]

	// - Partition ID 4
	partition_id := binary.BigEndian.Uint32(b[:4])
	log.Printf("partition: partition id %v", partition_id)
	b = b[4:]

	// - Topic UUID 16
	topic_uuid, _ := uuid.FromBytes(b[:16])
	log.Printf("partition: uuid %v", topic_uuid)
	b = b[16:]

	// - Length of replica array 1
	replica_arr_length := int(b[0]) - 1
	log.Printf("partition: replica_arr_length %v", replica_arr_length)
	b = b[1:]

	// - Replica Array 4
	replica_arr := b[:4]
	log.Printf("partition: replica_arr %v", replica_arr)
	b = b[4:]

	// - Length of In Sync Replica array 1
	in_sync_replica_arr_length := int(b[0]) - 1
	log.Printf("partition: in_sync_replica_arr_length %v", in_sync_replica_arr_length)
	b = b[1:]
	// - In Sync Replica Array 4 (30)
	in_sync_replica_arr := b[:4]
	log.Printf("partition: in_sync_replica_arr_length %v", in_sync_replica_arr)
	b = b[4:]

	// - Length of Removing Replicas array 1
	rmv_replica_arr_length := int(b[0]) - 1
	log.Printf("partition: rmv_replica_arr_length %v", rmv_replica_arr_length)
	b = b[1:]

	// - Length of Adding Replicas array 1
	add_replica_arr_length := int(b[0]) - 1
	log.Printf("partition: add_replica_arr_length %v", add_replica_arr_length)
	b = b[1:]

	// - Leader 4
	leader := binary.BigEndian.Uint32(b[:4])
	log.Printf("partition: leader %v", leader)
	b = b[4:]

	// - Leader Epoch 4
	leader_epoch := binary.BigEndian.Uint32(b[:4])
	log.Printf("partition: leader_epoch %v", leader_epoch)
	b = b[4:]

	// - Partition Epoch 4 (14)
	partition_epoch := binary.BigEndian.Uint32(b[:4])
	log.Printf("partition_epoch %v", partition_epoch)
	b = b[4:]

	// - Length of Directories array 1
	dir_arr_len := b[0]
	log.Printf("partition: Length of Directories %v", dir_arr_len)
	b = b[1:]

	// - Directories Array 16
	dir_arr, _ := uuid.FromBytes(b[:16])
	log.Printf("partition: Directories %v", dir_arr)
	b = b[16:]

	// - Tagged Fields Count 1
	tag_field_cnt := b[0]
	log.Printf("partition: tag_field_cnt %v", tag_field_cnt)
	b = b[1:]
	// - Headers Array Count 1
	b = b[1:]

	return models.Partition{
		PartitionId:                uint32(partition_id),
		LengthOfReplicaArray:       uint8(replica_arr_length),
		ReplicaArray:               replica_arr,
		LengthOfInSyncReplicaArray: uint8(in_sync_replica_arr_length),
		InSyncReplicaArray:         in_sync_replica_arr,
		LengthOfRemovingReplicas:   uint8(rmv_replica_arr_length),
		LengthOfAddingReplicas:     uint8(add_replica_arr_length),
		Leader:                     leader,
		LeaderEpoch:                leader_epoch,
		PartitionEpoch:             partition_epoch,
		LengthOfDirectoriesArray:   dir_arr_len,
		DirectoriesArray:           dir_arr,
		TaggedFieldsCount:          tag_field_cnt,
	}, b
}

func ParseTopic(b []byte) (models.Topic, []byte) {
	base_offset := binary.BigEndian.Uint64(b[:8])    // Extract first 8 bytes as uint64
	batch_length := binary.BigEndian.Uint32(b[8:12]) // Extract next 4 bytes as uint32
	log.Printf("Parse topic: Base offset: %v\tBatch length: %v", base_offset, batch_length)
	b = b[12:]

	b = b[4:] // Partition Leader Epoch 4
	b = b[1:] // Magic Byte 1
	b = b[4:] // CRC 4
	b = b[2:] // Attributes 2

	offset_delta := binary.BigEndian.Uint32(b[:4]) // Last Offset Delta 4
	log.Printf("Record batch has %v records (including record declaring topic)", offset_delta+1)
	b = b[4:]

	// Base Timestamp 8
	// Max Timestamp 8
	// Producer ID 8
	// Producer Epoch 2
	// Base Sequence 4
	// Record Length 4
	b = b[34:]

	// Topic Record
	size := protowire.DecodeZigZag(uint64(b[0]))
	log.Printf("topic record has length %d", size)

	// - Length 1
	// - Attributes 1
	// - Timestamp Delta 1
	// - Offset Delta 1
	b = b[4:]

	// - Key Length 1
	key_length := protowire.DecodeZigZag(uint64(b[0]))
	log.Printf("Key length %d", int8(key_length))
	b = b[1:]
	// - Key 0
	if key_length >= 0 {
		b = b[key_length:]
	}
	// - Value Length 1
	b = b[1:]

	log.Printf("Frame version %v", b[0]) // - Frame Version 1
	log.Printf("Type %x", b[1])          // - Type 1
	log.Printf("Version %v", b[2])       // - Version 1

	name_length := int(b[3]) - 1                                         // - Name length 1
	topic_name := string(b[4 : 4+name_length])                           // - Topic Name
	topic_uuid, _ := uuid.FromBytes(b[4+name_length : 4+name_length+16]) // - Topic UUID 16
	log.Printf("name_length %v - name %v - uuid %v", name_length, topic_name, topic_uuid)

	topic := models.Topic{TopicName: topic_name, TopicId: topic_uuid.String()}

	// - Tagged Fields Count 1
	// - Headers Array Count 1
	b = b[4+name_length+16+2:] // +2 is for tagged fields count and headers array count

	var partition models.Partition
	for i := 0; i < int(offset_delta); i++ {
		log.Printf("***EXTRACTING PARTITION #%v", i)
		partition, b = ParsePartition(b)
		topic.Partitions = append(topic.Partitions, partition)
	}

	log.Printf("FINISHED PARSING ITEM %v - remaining %v", topic.TopicName, len(b))

	return topic, b
}

func ParseLogFile(fp string) error {
	models.DummyDb = make(map[string]models.Topic)
	file, err := os.Open(fp)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err = file.Close(); err != nil {
			log.Fatal(err)
		}
	}()

	b, err := io.ReadAll(file)

	var base_offset uint64
	var batch_length uint32

	// Record Batch 1
	base_offset = binary.BigEndian.Uint64(b[:8])    // Extract first 8 bytes as uint64
	batch_length = binary.BigEndian.Uint32(b[8:12]) // Extract next 4 bytes as uint32
	log.Printf("Record batch 1: Base offset: %v\tBatch length: %v", base_offset, batch_length)
	b = b[12+batch_length:]

	var topic models.Topic

	for len(b) > 0 {
		log.Printf("\n********EXTRACTING TOPIC #%v", len(b))
		topic, b = ParseTopic(b)
		models.DummyDb[topic.TopicName] = topic
	}

	return nil
}
