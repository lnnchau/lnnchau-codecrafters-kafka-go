package handler

import (
	"encoding/binary"
	"io"
	"log"

	"github.com/codecrafters-io/kafka-starter-go/app/common"
)

type ApiVersionHandler struct{}

type ApiVersion struct {
	tag_buffer       int8
	api_keys         []ApiKey
	throttle_time_ms int32
}

type ApiKey struct {
	request_api_key int16
	min_version     int16
	max_version     int16
	tag_buffer      int8
}

func (handler *ApiVersionHandler) getAPIKeys(req common.RequestMessage) []ApiKey {
	return []ApiKey{
		ApiKey{
			request_api_key: req.Header.RequestApiKey,
			min_version:     0,
			max_version:     4,
			tag_buffer:      0,
		},
		ApiKey{
			request_api_key: 75,
			min_version:     0,
			max_version:     0,
			tag_buffer:      0,
		},
	}
}

func (handler *ApiVersionHandler) createAPIVersionObject(req common.RequestMessage) *ApiVersion {
	return &ApiVersion{
		api_keys:         handler.getAPIKeys(req),
		throttle_time_ms: 0,
		tag_buffer:       0,
	}
}

func (handler *ApiVersionHandler) validate(req common.RequestMessage) common.Error {
	error_code := 0
	if req.Header.RequestApiVerison != 4 {
		error_code = 35
	}

	return common.Error(error_code)
}

func (handler *ApiVersionHandler) Process(w io.Writer, req common.RequestMessage) {
	// ApiVersions Response (Version: 4) => error_code [api_keys] throttle_time_ms TAG_BUFFER
	// error_code => INT16
	// api_keys => api_key min_version max_version TAG_BUFFER
	//   api_key => INT16
	//   min_version => INT16
	//   max_version => INT16
	// throttle_time_ms => INT32

	error_code := handler.validate(req)

	api_version_object := handler.createAPIVersionObject(req)

	body := make([]byte, 0)

	body = binary.BigEndian.AppendUint32(body, uint32(req.Header.CorrelationId))
	body = binary.BigEndian.AppendUint16(body, uint16(error_code))
	body = append(body, uint8(1+len(api_version_object.api_keys)))

	log.Print(api_version_object.api_keys)
	for _, item := range api_version_object.api_keys {
		body = binary.BigEndian.AppendUint16(body, uint16(item.request_api_key))
		body = binary.BigEndian.AppendUint16(body, uint16(item.min_version))
		body = binary.BigEndian.AppendUint16(body, uint16(item.max_version))
		body = append(body, 0)
	}

	body = binary.BigEndian.AppendUint32(body, uint32(api_version_object.throttle_time_ms))
	body = append(body, 0)

	log.Print("Length of body")
	log.Print(len(body))

	output := make([]byte, 0)
	output = binary.BigEndian.AppendUint32(output, uint32(len(body)))
	output = append(output, body...)

	w.Write(output)
}
