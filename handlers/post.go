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
	bucketName    = "jtb-source-structures"
	credsFilePath = os.Getenv("GOOGLE_APPLICATION_CREDENTIALS")
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
	jsonFile := fmt.Sprintf("%v.json", jtaData.TableName)
	avroFiles := []string{avscFile, jsonFile}

	// CREATE A FOLDER FOR THE DATASET
	err = os.Mkdir(jtaData.DatasetName, os.ModePerm)
	if err != nil {
		log.Printf("ERROR CREATING FOLDER: %v", err.Error())
		return
	}

	// CREATE A STORAGE CLIENT TO TEST THE AUTH
	storClient, err := gcp.GetStorageClient(credsFilePath)
	if err != nil {
		log.Printf("ERROR CREATING GCS CLIENT: %v", err.Error())
		data.ErrorWithJSON(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if storClient == nil {
		data.ErrorWithJSON(w, "Authentication JSON passed invalid.", http.StatusBadRequest)
		return
	}
	// CREATE BUCKET IF NOT BEEN MADE BEFORE
	err = gcp.CreateBucket(storClient, jtaData.ProjectID, bucketName)
	if err != nil {
		if e, ok := err.(*googleapi.Error); ok {
			if e.Code != 409 {
				log.Printf("ERROR CREATING BUCKET: %v", err.Error())
				data.ErrorWithJSON(w, err.Error(), http.StatusInternalServerError)
				return
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
			data.ErrorWithJSON(w, err.Error(), http.StatusInternalServerError)
			log.Printf("ERROR DOWNLOADING BLOB FROM GCP: %v", err.Error())
			return
		}
	}
	// BEGIN PARSING THE REQUEST USING THE AVRO MODULE, THIS FORMATS DATA AND CREATES SCHEMA
	s, formattedData, err := avro.ParseRequest(jtaData)
	if err != nil {
		data.ErrorWithJSON(w, err.Error(), http.StatusInternalServerError)
		return
	}

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
		data.ErrorWithJSON(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// UPLOAD FILES TO BUCKET
	for _, f := range avroFiles {
		err = gcp.UploadBlobToStorage(storClient, bucketName, jtaData.DatasetName, f)
		if err != nil {
			log.Printf("ERROR UPLOADING AVRO FILE: %v %v", f, err.Error())
		}

	}

	// CREATING BQ CLIENT
	bqClient, err := gcp.GetBQClient(credsFilePath, jtaData.ProjectID)
	if err != nil {
		log.Printf("ERROR CREATING BQ CLIENT: %v", err.Error())
		data.ErrorWithJSON(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if bqClient == nil {
		data.ErrorWithJSON(w, "Authentication JSON passed invalid.", http.StatusBadRequest)
		return
	}

	// CREATING TABLE, IF IT EXISTS, ERROR IS SKIPPED WITHIN FUNC
	err = gcp.CreateTable(bqClient, jtaData.DatasetName, jtaData.TableName)
	if err != nil {
		data.ErrorWithJSON(w, err.Error(), http.StatusBadRequest)
		return
	}

	// ADD ANY NEW SCHEMA USING SCHEMA FIELD NAMES
	err = gcp.EnsureSchema(bqClient, jtaData.DatasetName, jtaData.TableName, s)
	if err != nil {
		data.ErrorWithJSON(w, err.Error(), http.StatusBadRequest)
		return
	}

	// THIS IS TEST LOGIC TO RETURN SAME ITEM
	sJSON, err := s.ToJSON()
	if err != nil {
		log.Fatalln(err.Error())
	}
	w.Header().Add("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, err = w.Write(sJSON)
	if err != nil {
		log.Fatalln("COULD NOT WRITE RESPONSE")
	}

	// DELETE THE FOLDER WHEN DONE
	err = os.Remove(jtaData.DatasetName)
	if err != nil {
		log.Printf("ERROR DELETING FOLDER: %v", jtaData.DatasetName)
	}
	log.Println("Completed request")
}
