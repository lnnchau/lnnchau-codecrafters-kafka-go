package common

import (
	"encoding/binary"
)

func ParseRequest(request_bytes []byte) RequestMessage {
	message_size := binary.BigEndian.Uint32(request_bytes[0:4])
	request_api_key := binary.BigEndian.Uint16(request_bytes[4:6])
	request_api_version := binary.BigEndian.Uint16(request_bytes[6:8])
	correlation_id := binary.BigEndian.Uint32(request_bytes[8 : 8+4])

	client_id_length := binary.BigEndian.Uint16(request_bytes[12 : 12+2])
	client_id := request_bytes[14 : 14+client_id_length]
	// skip 1 for tag buffer
	body := request_bytes[14+client_id_length+1:]

	return RequestMessage{
		MessageSize: int32(message_size),
		Header: RequestHeaderV2{
			RequestApiKey:     int16(request_api_key),
			RequestApiVerison: int16(request_api_version),
			CorrelationId:     int32(correlation_id),
			ClientId:          string(client_id),
		},
		Body: body,
	}
}
