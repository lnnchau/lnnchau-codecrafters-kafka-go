package main

import (
	"encoding/binary"
)

type Utility interface {
	convertToBytes() []byte
}

type RequestMessage struct {
	message_size int32
	header       RequestHeaderV2
	body         Body
}

type ResponseMessage struct {

	// ApiVersions Response (Version: 4) => error_code [api_keys] throttle_time_ms TAG_BUFFER
	// error_code => INT16
	// api_keys => api_key min_version max_version TAG_BUFFER
	//   api_key => INT16
	//   min_version => INT16
	//   max_version => INT16
	// throttle_time_ms => INT32

	message_size int32

	// header
	correlation_id int32

	// body
	error_code int16

	tag_buffer int16

	// api keys
	request_api_key int16
	min_version     int16
	max_version     int16

	throttle_time_ms int32
}

type RequestHeaderV2 struct {
	request_api_key     int16 // The API key for the request
	request_api_version int16 // The version of the API for the request
	correlation_id      int32 // A unique identifier for the request
	// client_id           string   // The client ID for the request
	// TAG_BUFFER          []string // Optional tagged fields
}

type Body struct {
	content string
}

func NewRequestMessage(request_bytes []byte) *RequestMessage {
	message_size := binary.BigEndian.Uint32(request_bytes[0:4])
	request_api_key := binary.BigEndian.Uint16(request_bytes[4:6])
	request_api_version := binary.BigEndian.Uint16(request_bytes[6:8])
	correlation_id := binary.BigEndian.Uint32(request_bytes[8 : 8+4])

	return &RequestMessage{
		message_size: int32(message_size),
		header: RequestHeaderV2{
			request_api_key:     int16(request_api_key),
			request_api_version: int16(request_api_version),
			correlation_id:      int32(correlation_id),
		},
	}
}

func NewResponseMessage(correlation_id int32, error_code int16, request_api_key int16) *ResponseMessage {
	return &ResponseMessage{
		message_size:     16,
		correlation_id:   correlation_id,
		error_code:       error_code,
		request_api_key:  request_api_key,
		min_version:      0,
		max_version:      4,
		throttle_time_ms: 0,
		tag_buffer:       0,
	}
}

func (message *ResponseMessage) convertToBytes() []byte {
	// ApiVersions Response (Version: 4) => error_code [api_keys] throttle_time_ms TAG_BUFFER
	// error_code => INT16
	// api_keys => api_key min_version max_version TAG_BUFFER
	//   api_key => INT16
	//   min_version => INT16
	//   max_version => INT16
	// throttle_time_ms => INT32

	output := make([]byte, 0)

	output = binary.BigEndian.AppendUint32(output, uint32(message.message_size))
	output = binary.BigEndian.AppendUint32(output, uint32(message.correlation_id))
	output = binary.BigEndian.AppendUint16(output, uint16(message.error_code))
	output = binary.BigEndian.AppendUint16(output, uint16(message.request_api_key))
	output = binary.BigEndian.AppendUint16(output, uint16(message.min_version))
	output = binary.BigEndian.AppendUint16(output, uint16(message.max_version))
	output = binary.BigEndian.AppendUint16(output, uint16(message.tag_buffer))
	output = binary.BigEndian.AppendUint32(output, uint32(message.throttle_time_ms))
	output = binary.BigEndian.AppendUint16(output, uint16(message.tag_buffer))

	return output
}
