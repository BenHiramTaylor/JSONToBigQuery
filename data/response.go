package data

import (
	"fmt"
	"net/http"
)

func ErrorWithJSON(w http.ResponseWriter, message string, httpErrorCode int) {
	messageJSON := []byte(fmt.Sprintf("{\"error\":\"%v\"}", message))
	w.Header().Add("Content-Type", "application/json")
	w.WriteHeader(httpErrorCode)
	w.Write(messageJSON)
}
