package handlers

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"

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
	err = os.Mkdir(jtaData.DatasetName, os.ModePerm)
	if err != nil {
		log.Printf("Error creating folder: %v", err.Error())
	}
	s, err := avro.ParseRequest(jtaData)
	err = s.ToFile(jtaData.DatasetName)
	if err != nil {
		log.Printf("Error dumping schema to avsc file: %v", err.Error())
	}
	// THIS IS TEST LOGIC TO RETURN SAME ITEM
	sJSON, err := json.Marshal(s)
	if err != nil {
		log.Fatalln(err.Error())
	}
	w.Header().Add("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	w.Write(sJSON)
	// TODO PARSE LOGIC HERE
}
