package data

import (
	"encoding/json"
	"log"
	"net/http"
)

// Response represents a basic http json response
type Response struct {
	Status  string `json:"status"`
	Content string `json:"content"`
}

// NewResponse is a contstructor func to return a new response object
func NewResponse(status, content string) *Response {
	return &Response{Status: status, Content: content}
}

// ToJSON Dumps the struct into JSON bytes
func (r *Response) ToJSON() ([]byte, error) {
	return json.Marshal(r)
}

// RespondWithJSON Takes a responseWriter, status, message, and error code, and
// responds to the http call.
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
