package models

type Partition struct {
	// - Partition ID 4
	PartitionId uint32

	// - Topic UUID 16
	// TopicUUID [16]byte

	// - Length of replica array 1
	LengthOfReplicaArray uint8
	// - Replica Array 4
	ReplicaArray uint32

	// - Length of In Sync Replica array 1
	LengthOfInSyncReplicaArray uint8
	// - In Sync Replica Array 4
	InSyncReplicaArray uint32

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
