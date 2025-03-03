package models

// only need to extract neccessary info only

type BatchMetadata struct {
	// Base Offset 8

	// Batch Length 4

	// Partition Leader Epoch 4

	// Magic Byte 1

	// CRC 4

	// Attributes 2

	// Last Offset Delta 4

	// Base Timestamp 8

	// Max Timestamp 8

	// Producer ID 8

	// Producer Epoch 2

	// Base Sequence 4

	// Records Length 4
}

/*****************/

type LogRecord struct {
	// - Frame Version 1
	// - Type 1
	// - Version 1
	FrameVersion  uint8
	Type          uint8
	Version       uint8
	RecordContent Serializer
}
