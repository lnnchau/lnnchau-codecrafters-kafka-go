package common

import (
	"encoding/binary"
)

func ParseRequest(request_bytes []byte) RequestMessage {
	message_size := binary.BigEndian.Uint32(request_bytes[0:4])

	header := RequestHeaderV2{}
	body_bytes, _ := header.Deserialize(request_bytes[4:])

	return RequestMessage{
		MessageSize: int32(message_size),
		Header:      header,
		Body:        body_bytes,
	}
}
