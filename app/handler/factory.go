package handler

func GetApiHandlerByKey(apiVersion int16) Handler {
	switch apiVersion {
	case 18:
		return &ApiVersionHandler{}
	case 75:
		return &DescribeTopicPartitionsHandler{}

	}

	return nil
}
