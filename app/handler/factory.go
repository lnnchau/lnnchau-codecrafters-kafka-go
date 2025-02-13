package handler

func GetApiHandlerByKey(apiVersion int16) Handler {
	return &ApiVersionHandler{}
}
