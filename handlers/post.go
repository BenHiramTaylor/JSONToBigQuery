package handlers

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"github.com/BenHiramTaylor/JSONToBigQuery/avro"
	"github.com/BenHiramTaylor/JSONToBigQuery/data"
	"github.com/go-playground/validator"
)

func JtBPost(w http.ResponseWriter, r *http.Request) {
	jtaData := data.NewJTB()

	if err := jtaData.LoadFromJSON(r); err != nil {
		data.ErrorWithJSON(w, fmt.Sprintf("JSON data is invalid: %v", err.Error()), http.StatusBadRequest)
		return
	}
	err := jtaData.Validate()
	if err != nil {
		var errSlice []string
		for _, err := range err.(validator.ValidationErrors) {
			errSlice = append(errSlice, fmt.Sprintf("Key: %v is invalid, got value: %v", err.Field(), err.Value()))
		}
		data.ErrorWithJSON(w, errSlice, http.StatusBadRequest)
		return
	}
	log.Printf("%#v", jtaData)
	s, err := avro.ParseRequest(jtaData)
	// THIS IS TEST LOGIC TO RETURN SAME ITEM
	sJSON, err := json.Marshal(s)
	if err != nil {
		log.Fatalln(err.Error())
	}
	w.WriteHeader(http.StatusOK)
	w.Header().Add("Content-Type", "application/json; charset=utf-8")
	w.Write(sJSON)
	// TODO PARSE LOGIC HERE
}
