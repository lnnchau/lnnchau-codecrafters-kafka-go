package handler

import (
	"io"

	"github.com/codecrafters-io/kafka-starter-go/app/common"
)

type Handler interface {
	validate(common.RequestMessage) common.Error
	Process(io.Writer, common.RequestMessage)
}
