package models

import (
	"encoding/binary"
)

// Requests
type RequestHeaderV2 struct {
	// Request Header v2 => request_api_key request_api_version correlation_id client_id TAG_BUFFER
	// request_api_key => INT16
	// request_api_version => INT16
	// correlation_id => INT32
	// client_id => NULLABLE_STRING

	RequestApiKey     int16  // The API key for the request
	RequestApiVerison int16  // The version of the API for the request
	CorrelationId     int32  // A unique identifier for the request
	ClientId          string // The client ID for the request
	// TAG_BUFFER        []string // Optional tagged fields
}

func (header *RequestHeaderV2) Serialize() ([]byte, error) {
	return nil, nil
}

func (header *RequestHeaderV2) Deserialize(request_bytes []byte) ([]byte, error) {
	cursor := 0
	field_length := 2
	request_api_key := binary.BigEndian.Uint16(request_bytes[cursor : cursor+field_length])
	cursor += field_length

	field_length = 2
	request_api_version := binary.BigEndian.Uint16(request_bytes[cursor : cursor+field_length])
	cursor += field_length

	field_length = 4
	correlation_id := binary.BigEndian.Uint32(request_bytes[cursor : cursor+field_length])
	cursor += field_length

	field_length = 2
	client_id_length := binary.BigEndian.Uint16(request_bytes[cursor : cursor+field_length])
	cursor += field_length

	field_length = int(client_id_length)
	client_id := request_bytes[cursor : cursor+field_length]
	cursor += field_length

	// TAG BUGGER
	field_length = 1
	cursor += field_length

	header.RequestApiKey = int16(request_api_key)
	header.RequestApiVerison = int16(request_api_version)
	header.CorrelationId = int32(correlation_id)
	header.ClientId = string(client_id)

	// log.Printf("Remaining bytes %v %v %v", request_bytes[cursor:], cursor, cursor+field_length)

	return request_bytes[cursor:], nil
}

type RequestMessage struct {
	MessageSize int32
	Header      RequestHeaderV2
	Body        []byte
}
