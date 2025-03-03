package handler

import (
	"io"

	"github.com/codecrafters-io/kafka-starter-go/app/common"
	"github.com/codecrafters-io/kafka-starter-go/app/models"
)

type Handler interface {
	validate(models.RequestMessage) common.Error
	Process(io.Writer, models.RequestMessage)
}
