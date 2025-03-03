package models

import (
	"encoding/binary"

	"github.com/codecrafters-io/kafka-starter-go/app/common"
)

type Partition struct {
	ErrorCode common.Error

	// - Partition ID 4
	PartitionId uint32

	// - Topic UUID 16
	// TopicUUID [16]byte

	// - Length of replica array 1
	LengthOfReplicaArray uint8
	// - Replica Array 4
	ReplicaArray []byte

	// - Length of In Sync Replica array 1
	LengthOfInSyncReplicaArray uint8
	// - In Sync Replica Array 4
	InSyncReplicaArray []byte

	// - Length of Removing Replicas array 1
	LengthOfRemovingReplicas uint8
	// - Length of Adding Replicas array 1
	LengthOfAddingReplicas uint8

	// - Leader 4
	Leader uint32
	// - Leader Epoch 4
	LeaderEpoch uint32
	// - Partition Epoch 4
	PartitionEpoch uint32

	// - Length of Directories array 1
	LengthOfDirectoriesArray uint8
	// - Directories Array 16
	DirectoriesArray [16]byte

	// - Tagged Fields Count 1
	TaggedFieldsCount uint8
}

func (partition *Partition) Deserialize(body []byte) ([]byte, error) {
	cursor := 0
	return body[cursor:], nil
}

func (partition *Partition) Serialize() ([]byte, error) {
	output := make([]byte, 0)

	output = binary.BigEndian.AppendUint16(output, uint16(partition.ErrorCode)) // Error Code

	output = binary.BigEndian.AppendUint32(output, partition.PartitionId) // Partition Index 4

	output = binary.BigEndian.AppendUint32(output, partition.Leader) //	Leader ID

	output = binary.BigEndian.AppendUint32(output, partition.LeaderEpoch) // Leader Epoch

	// Replica Nodes
	output = append(output, 0x02)                  // - Array Length
	output = append(output, []byte{0, 0, 0, 1}...) // - Replica Node

	// ISR Nodes
	output = append(output, 0x02)                  // - Array Length
	output = append(output, []byte{0, 0, 0, 1}...) // - ISR Node

	// Eligible Leader Replicas
	output = append(output, 0x01) // Array Length

	// Last Known ELR
	output = append(output, 0x01) // Array Length
	// Offline Replicas
	output = append(output, 0x01) // Array Length

	// Tag Buffer
	output = append(output, 0x00)
	return output, nil
}
