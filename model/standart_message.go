package model

type MessageRabbit struct {
	RequestInfo RequestInfo `json:"request_info"`
	Content     interface{} `json:"content"`
}

type RequestInfo struct {
	RequestID      string      `json:"request_id"`
	OriginalSource string      `json:"original_source"`
	Source         string      `json:"source"`
	ClientData     interface{} `json:"client_data"`
}
