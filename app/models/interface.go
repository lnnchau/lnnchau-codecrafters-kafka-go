package models

type Serializer interface {
	Serialize() ([]byte, error)

	// receive stream of bytes, return the remaining unprocessed stream of bytes
	Deserialize([]byte) ([]byte, error)
}
