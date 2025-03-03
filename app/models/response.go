package models

import (
	"encoding/binary"
)

// Requests
type RequestResponseHeaderV0 struct {
	CorrelationId int32 // A unique identifier for the request
	// TAG_BUFFER        []string // Optional tagged fields
}

func (header *RequestResponseHeaderV0) Serialize() ([]byte, error) {
	output := make([]byte, 0)
	output = binary.BigEndian.AppendUint32(output, uint32(header.CorrelationId))
	output = append(output, 0x00) // tag buffer
	return output, nil
}

func (header *RequestResponseHeaderV0) Deserialize(bytes []byte) ([]byte, error) {
	return nil, nil
}

type ResponseMessage struct {
	MessageSize int32
	Header      Serializer
	Body        Serializer
}

func (resp *ResponseMessage) Serialize() ([]byte, error) {
	response := make([]byte, 0)
	header_bytes, _ := resp.Header.Serialize()
	body_bytes, _ := resp.Body.Serialize()
	response = append(response, header_bytes...)
	response = append(response, body_bytes...)

	output := make([]byte, 0)
	output = binary.BigEndian.AppendUint32(output, uint32(len(response)))
	output = append(output, response...)
	return output, nil
}

func (resp *ResponseMessage) Deserialize(bytes []byte) ([]byte, error) {
	return nil, nil
}
