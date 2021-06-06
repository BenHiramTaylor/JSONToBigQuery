package data

import (
	"encoding/json"
	"log"
	"net/http"
)

type Response struct {
	Status  string      `json:"status"`
	Content interface{} `json:"content"`
}

func NewResponse(status string, content interface{}) *Response {
	return &Response{Status: status, Content: content}
}

func (r *Response) ToJson() ([]byte, error) {
	return json.Marshal(r)
}

func ErrorWithJSON(w http.ResponseWriter, message interface{}, httpErrorCode int) {
	r := NewResponse("error", message)
	rJson, err := r.ToJson()
	if err != nil {
		log.Fatalf("Could not marshal JSON: %#v", r)
	}
	w.Header().Add("Content-Type", "application/json")
	w.WriteHeader(httpErrorCode)
	w.Write(rJson)
}
