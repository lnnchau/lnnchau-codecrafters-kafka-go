package common

type Error int16

type RequestHeaderV2 struct {
	RequestApiKey     int16  // The API key for the request
	RequestApiVerison int16  // The version of the API for the request
	CorrelationId     int32  // A unique identifier for the request
	ClientId          string // The client ID for the request
	// TAG_BUFFER        []string // Optional tagged fields
}

type RequestMessage struct {
	MessageSize int32
	Header      RequestHeaderV2
	Body        []byte
}
