package handlers

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"

	"github.com/BenHiramTaylor/JSONToBigQuery/avro"
	"github.com/BenHiramTaylor/JSONToBigQuery/data"
	"github.com/BenHiramTaylor/JSONToBigQuery/gcp"
	"github.com/go-playground/validator"
	"google.golang.org/api/googleapi"
)

var (
	bucketName = "jtb-source-structures"
)

func JtBPost(w http.ResponseWriter, r *http.Request) {
	// CONSTRUCT NEW JTB INSTANCE
	jtaData := data.NewJTB()

	// LOAD THE JSON REQUEST INTO THE INSTANCE
	if err := jtaData.LoadFromJSON(r); err != nil {
		data.ErrorWithJSON(w, fmt.Sprintf("JSON data is invalid: %v", err.Error()), http.StatusBadRequest)
		return
	}
	// VALIDATE THE JSON USING THE VALIDATE TAGS AND RETURN A LIST OF ERRORS IF IT FAILS
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

	// CREATE LIST OF FILE NAMES
	avscFile := fmt.Sprintf("%v.avsc", jtaData.TableName)
	bqsFile := fmt.Sprintf("%v.bqs", jtaData.TableName)
	jsonFile := fmt.Sprintf("%v.json", jtaData.TableName)
	avroFiles := []string{avscFile, bqsFile, jsonFile}

	// CREATE A FOLDER FOR THE DATASET
	err = os.Mkdir(jtaData.DatasetName, os.ModePerm)
	if err != nil {
		log.Printf("ERROR CREATING FOLDER: %v", err.Error())
	}

	// CREATE A STORAGE CLIENT TO TEST THE AUTH
	storClient, err := gcp.GetClient(jtaData.AuthJSON)
	if err != nil {
		log.Printf("ERROR CREATING GCS CLIENT: %v", err.Error())

	}
	if storClient == nil {
		data.ErrorWithJSON(w, "Authentication JSON passed invalid.", http.StatusBadRequest)
		return
	}
	// CREATE BUCKET IF NOT BEEN MADE BEFORE
	err = gcp.CreateBucket(storClient, jtaData.ProjectID, bucketName)
	if err != nil {
		if e, ok := err.(*googleapi.Error); ok {
			if e.Code == 409 {
				log.Println("BUCKET ALREADY EXISTS")
			} else {
				log.Printf("ERROR CREATING BUCKET: %v", err.Error())
			}
		}
	}

	// DOWNLOAD OR CREATE FILES NEEDED FROM GCS
	for _, v := range avroFiles {
		err = gcp.DownloadBlobFromStorage(storClient, bucketName, jtaData.DatasetName, v)
		if err != nil {
			if err.Error() == "storage: object doesn't exist" {
				continue
			}
			log.Printf("ERROR DOWNLOADING BLOB FROM GCP: %v", err.Error())
		}
	}
	// BEGIN PARSING THE REQUEST USING THE AVRO MODULE, THIS FORMATS DATA AND CREATES SCHEMA
	s, formattedData, err := avro.ParseRequest(jtaData)

	// DUMP THE AVSC TO FILE
	err = s.ToFile(jtaData.DatasetName)
	if err != nil {
		log.Printf("ERROR DUMPING SCHEMA TO AVSC FILE: %v", err.Error())
	}

	// DUMP THE FORMATTED RECORDS TO JSON
	jsonData, err := json.Marshal(formattedData)
	if err != nil {
		log.Printf("ERROR MARSHALLING JSON DATA: %v", err.Error())
	}
	err = ioutil.WriteFile(fmt.Sprintf("%v/%v", jtaData.DatasetName, jsonFile), jsonData, 0644)
	if err != nil {
		log.Printf("ERROR DUMPING FORMATTED JSON TO FILE: %v", err.Error())
	}

	// TESTING UPLOADING TO BUCKET
	for _, f := range avroFiles {
		err = gcp.UploadBlobToStorage(storClient, bucketName, jtaData.DatasetName, f)
		if err != nil {
			log.Printf("ERROR UPLOADING AVRO FILE: %v %v", f, err.Error())
		}

	}

	// THIS IS TEST LOGIC TO RETURN SAME ITEM
	sJSON, err := s.ToJSON()
	if err != nil {
		log.Fatalln(err.Error())
	}
	w.Header().Add("Content-Type", "applicatio,n/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	w.Write(sJSON)
	// TODO PARSE LOGIC HERE

	// DELETE THE FOLDER WHEN DONE
	err = os.Remove(jtaData.DatasetName)
	if err != nil {
		log.Printf("ERROR DELETING FOLDER: %v", jtaData.DatasetName)
	}
}
