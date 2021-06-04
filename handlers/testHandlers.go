package handlers

import (
	"fmt"
	"log"
	"net/http"

	"github.com/BenHiramTaylor/JSONToBigQuery/dataTypes"
	"github.com/go-playground/validator"
)

func TestJSONPost(w http.ResponseWriter, r *http.Request) {
	jtaData := dataTypes.NewJTB()

	if err := jtaData.LoadFromJSON(r); err != nil {
		ErrorWithJSON(w, fmt.Sprintf("JSON data is invalid: %v", err.Error()), http.StatusBadRequest)
		return
	}
	err := jtaData.Validate()
	if err != nil {
		var errSlice []string
		for _, err := range err.(validator.ValidationErrors) {
			errSlice = append(errSlice, fmt.Sprintf("Key: %v is invalid, got value: %v", err.Field(), err.Value()))
		}
		ErrorWithJSON(w, errSlice, http.StatusBadRequest)
		return
	}
	resp, err := jtaData.DumpToJSON()
	if err != nil {
		log.Fatalln("Can't Marshal JSON.")
	}
	w.Header().Add("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(resp)
}
