package handlers

import (
	"log"
	"net/http"

	"github.com/BenHiramTaylor/JSONToBigQuery/dataTypes"
)

func ErrorWithJSON(w http.ResponseWriter, message interface{}, httpErrorCode int) {
	var r = dataTypes.Response{Status: "error", Content: message}
	rJson, err := r.ToJson()
	if err != nil {
		log.Fatalf("Could not marshal JSON: %#v", r)
	}
	w.Header().Add("Content-Type", "application/json")
	w.WriteHeader(httpErrorCode)
	w.Write(rJson)
}
