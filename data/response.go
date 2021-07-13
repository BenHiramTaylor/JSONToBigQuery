package data

import (
	"encoding/json"
	"log"
	"net/http"
)

type Response struct {
	Status  string `json:"status"`
	Content string `json:"content"`
}

func NewResponse(status, content string) *Response {
	return &Response{Status: status, Content: content}
}

func (r *Response) ToJSON() ([]byte, error) {
	return json.Marshal(r)
}

func RespondWithJSON(w http.ResponseWriter, status, message string, httpErrorCode int) {
	r := NewResponse(status, message)
	responseJSON, err := r.ToJSON()
	if err != nil {
		log.Fatalf("Could not marshal JSON: %#v", r)
	}
	w.Header().Add("Content-Type", "application/json")
	w.WriteHeader(httpErrorCode)
	w.Write(responseJSON)
}
