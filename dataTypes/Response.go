package dataTypes

import "encoding/json"

type Response struct {
	Status  string      `json:"status"`
	Content interface{} `json:"content"`
}

func (r *Response) ToJson() ([]byte, error) {
	return json.Marshal(r)
}
