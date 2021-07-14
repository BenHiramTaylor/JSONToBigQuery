package handlers

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/storage"
	"github.com/BenHiramTaylor/JSONToBigQuery/avro"
	"github.com/BenHiramTaylor/JSONToBigQuery/data"
	"github.com/BenHiramTaylor/JSONToBigQuery/gcp"
	"github.com/go-playground/validator"
)

func JtBPost(w http.ResponseWriter, r *http.Request) {
	// CONSTRUCT NEW JTB INSTANCE
	jtb := data.NewJTB()

	// LOAD THE JSON REQUEST INTO THE INSTANCE
	if err := jtb.LoadFromJSON(r); err != nil {
		data.RespondWithJSON(w, "error", fmt.Sprintf("JSON data is invalid: %v", err.Error()), http.StatusBadRequest)
		return
	}
	// VALIDATE THE JSON USING THE VALIDATE TAGS AND RETURN A LIST OF ERRORS IF IT FAILS
	err := jtb.Validate()
	if err != nil {
		var errSlice []string
		for _, err := range err.(validator.ValidationErrors) {
			errSlice = append(errSlice, fmt.Sprintf("Key: %v is invalid, got value: %v", err.Field(), err.Value()))
		}
		data.RespondWithJSON(w, "error", strings.Join(errSlice, ","), http.StatusBadRequest)
		return
	}
	log.Printf("GOT REQUEST: %#v", jtb)

	// CREATE LIST OF FILE NAMES AND STORAGE WG
	var (
		avscFile       = fmt.Sprintf("%v.avsc", jtb.TableName)
		jsonFile       = fmt.Sprintf("%v.json", jtb.TableName)
		avroFile       = fmt.Sprintf("%v.avro", jtb.TableName)
		avroFiles      = []string{avscFile, jsonFile, avroFile}
		fileUploadWg   sync.WaitGroup
		fileDumpWg     sync.WaitGroup
		listMappingsWg sync.WaitGroup
		ListMappings   = make([]map[string]interface{}, 0)
	)

	// GET TIMESTAMP FORMAT OR USE DEFAULT
	if jtb.TimestampFormat == "" {
		jtb.TimestampFormat = time.RFC3339
	}

	// CREATE A FOLDER FOR THE DATASET
	err = os.Mkdir(jtb.DatasetName, os.ModePerm)
	if err != nil {
		log.Printf("ERROR CREATING FOLDER: %v", err.Error())
		return
	}

	// CREATE A STORAGE CLIENT TO TEST THE AUTH
	storageClient, err := gcp.GetStorageClient(data.CredsFilePath)
	if err != nil {
		log.Printf("ERROR CREATING GCS CLIENT: %v", err.Error())
		data.RespondWithJSON(w, "error", err.Error(), http.StatusInternalServerError)
		return
	}
	if storageClient == nil {
		data.RespondWithJSON(w, "error", "Authentication JSON passed invalid.", http.StatusBadRequest)
		return
	}
	defer storageClient.Close()

	// CREATING BQ CLIENT
	bigqueryClient, err := gcp.GetBQClient(data.CredsFilePath, jtb.ProjectID)
	if err != nil {
		log.Printf("ERROR CREATING BQ CLIENT: %v", err.Error())
		data.RespondWithJSON(w, "error", err.Error(), http.StatusInternalServerError)
		return
	}
	if bigqueryClient == nil {
		data.RespondWithJSON(w, "error", "Authentication JSON passed invalid.", http.StatusBadRequest)
		return
	}
	defer bigqueryClient.Close()

	// CREATE BUCKET IF NOT BEEN MADE BEFORE
	err = gcp.CreateBucket(storageClient, jtb.ProjectID, data.BucketName)
	if err != nil {
		data.RespondWithJSON(w, "error", err.Error(), http.StatusInternalServerError)
		return
	}

	// DOWNLOADS ALL THE FILES NEEDED FROM GCS IF THEY EXIST
	err = downloadFiles(storageClient, jtb.DatasetName, avroFiles)
	if err != nil {
		data.RespondWithJSON(w, "error", err.Error(), http.StatusInternalServerError)
		return
	}

	// BEGIN PARSING THE REQUEST USING THE AVRO MODULE, THIS FORMATS DATA AND CREATES SCHEMA
	s, formattedData, ListMappings, timestampFields, err := avro.ParseRequest(jtb)
	if err != nil {
		data.RespondWithJSON(w, "error", err.Error(), http.StatusInternalServerError)
		return
	}

	// START GOROUTINE FOR PARSING LIST MAPPINGS
	listMappingsWg.Add(1)
	go func() {
		parseListMappings(storageClient, bigqueryClient, jtb, ListMappings)
		listMappingsWg.Done()
	}()

	// PARSE OUR AVSC DATA THROUGH THE ENCODER
	avroBytes, err := s.WriteRecords(formattedData)
	if err != nil {
		data.RespondWithJSON(w, "error", err.Error(), http.StatusInternalServerError)
		return
	}

	// DUMP THE AVSC TO FILE
	fileDumpWg.Add(1)
	go func() {
		err = s.ToFile(jtb.DatasetName)
		if err != nil {
			log.Printf("ERROR DUMPING SCHEMA TO AVSC FILE: %v", err.Error())
		}
		fileDumpWg.Done()
	}()

	// DUMP THE FORMATTED RECORDS TO AVRO
	fileDumpWg.Add(1)
	go func() {
		err = ioutil.WriteFile(fmt.Sprintf("%v/%v", jtb.DatasetName, avroFile), avroBytes, 0644)
		if err != nil {
			data.RespondWithJSON(w, "error", err.Error(), http.StatusInternalServerError)
			return
		}
		fileDumpWg.Done()
	}()

	// WRITE THE FORMATTED DATA TO A JSON FILE
	fileDumpWg.Add(1)
	go func() {
		err = writeRecordsToFile(formattedData, jtb.DatasetName, jsonFile)
		if err != nil {
			data.RespondWithJSON(w, "error", err.Error(), http.StatusInternalServerError)
			return
		}
		fileDumpWg.Done()
	}()

	// WAIT FOR THE CONCURRENT FILE DUMPING TO FINISH
	fileDumpWg.Wait()

	// UPLOAD ALL THE FILES TO GCS
	fileUploadWg.Add(1)
	go func() {
		err = uploadFiles(storageClient, jtb.DatasetName, avroFiles)
		if err != nil {
			data.RespondWithJSON(w, "error", err.Error(), http.StatusInternalServerError)
			return
		}
		fileUploadWg.Done()
	}()

	// CREATE TABLE AND ADD ANY NEW SCHEMA USING SCHEMA FIELD NAMES
	err = gcp.PrepareTable(bigqueryClient, jtb.DatasetName, jtb.TableName, timestampFields, s)
	if err != nil {
		data.RespondWithJSON(w, "error", err.Error(), http.StatusBadRequest)
		return
	}

	// WAIT FOR THE FILE UPLOAD TO FINISH IF NOT DONE
	fileUploadWg.Wait()

	// LOAD THE DATA FROM GCS
	err = gcp.LoadAvroToTable(bigqueryClient, data.BucketName, jtb.DatasetName, jtb.TableName, avroFile)
	if err != nil {
		data.RespondWithJSON(w, "error", err.Error(), http.StatusInternalServerError)
		return
	}

	// RUN QUERY IF NOT BLANK
	err = jtb.ExecuteQuery()
	if err != nil {
		data.RespondWithJSON(w, "error", err.Error(), http.StatusInternalServerError)
		return
	}

	// WAIT FOR LISTMAPPINGS TO BE PARSED
	listMappingsWg.Wait()

	// RETURN CONFIRMATION RESPONSE
	data.RespondWithJSON(
		w,
		"success",
		fmt.Sprintf("Successfully Inserted %v number of rows into %v.%v.%v.", len(formattedData), jtb.ProjectID, jtb.DatasetName, jtb.TableName),
		http.StatusOK,
	)

	// DELETE THE FOLDER WHEN DONE
	err = os.Remove(jtb.DatasetName)
	if err != nil {
		log.Printf("ERROR DELETING FOLDER: %v", jtb.DatasetName)
	}
	log.Println("Completed request")
}

// If there are list mappings to parse, it will create the avro files, and load
// them to a generic ListMappings table in the dataset
func parseListMappings(storageClient *storage.Client, bigqueryClient *bigquery.Client, request *data.JTBRequest, ListMappings []map[string]interface{}) {
	var (
		storageWg  sync.WaitGroup
		listSchema = avro.Schema{
			Name:      fmt.Sprintf("%v.ListMappings.avro", request.TableName),
			Namespace: fmt.Sprintf("%v.ListMappings.avsc", request.TableName),
			Type:      "record",
			Fields: []avro.Field{
				{Name: "tableName", FieldType: []string{"string", "null"}},
				{Name: "idField", FieldType: []string{"string", "null"}},
				{Name: "Key", FieldType: []string{"string", "null"}},
				{Name: "Value", FieldType: []string{"string", "null"}},
			},
		}
	)
	log.Printf("LIST SCHEMA: %#v", listSchema)
	log.Printf("Finished Parsing all list mappings: %v", ListMappings)
	if len(ListMappings) == 0 {
		return
	}
	// PARSE OUR AVSC DATA THROUGH THE ENCODER
	avroBytes, err := listSchema.WriteRecords(ListMappings)
	if err != nil {
		log.Printf("ERROR PARSING LIST MAPPINGS: %v", err.Error())
		return
	}

	// DUMP THE FORMATTED RECORDS TO AVRO
	err = ioutil.WriteFile(fmt.Sprintf("%v/%v", request.DatasetName, listSchema.Name), avroBytes, 0644)
	if err != nil {
		log.Printf("ERROR WRITING LIST MAPPINGS AVRO FILE: %v", err.Error())
		return
	}

	storageWg.Add(1)
	go func() {
		// UPLOAD FILE TO BUCKET
		err = gcp.UploadBlobToStorage(storageClient, data.BucketName, request.DatasetName, listSchema.Name)
		if err != nil {
			log.Printf("ERROR UPLOADING AVRO FILE: %v %v", listSchema.Name, err.Error())
		}
		storageWg.Done()
	}()
	// CREATE TABLE AND ADD ANY NEW SCHEMA USING SCHEMA FIELD NAMES
	err = gcp.PrepareTable(bigqueryClient, request.DatasetName, "ListMappings", []string{}, listSchema)
	if err != nil {
		log.Println("ERROR PREPARING TABLE: ListMappings")
		return
	}
	storageWg.Wait()
	// LOAD THE DATA FROM GCS
	err = gcp.LoadAvroToTable(bigqueryClient, data.BucketName, request.DatasetName, "ListMappings", listSchema.Name)
	if err != nil {
		log.Printf("ERROR LOADING LISTMAPPINGS TABLE: %v", err.Error())
		return
	}
}

func downloadFiles(storageClient *storage.Client, dataSetName string, avroFiles []string) error {
	// DOWNLOAD OR CREATE FILES NEEDED FROM GCS
	for _, v := range avroFiles {
		// WE NEED TO CREATE A NEW AVRO FOR THE LOAD NOT RELOAD SAME DATA
		if strings.Contains(v, ".avro") {
			continue
		}
		err := gcp.DownloadBlobFromStorage(storageClient, data.BucketName, dataSetName, v)
		if err != nil {
			if err.Error() == "storage: object doesn't exist" {
				continue
			}
			log.Printf("ERROR DOWNLOADING BLOB FROM GCP: %v", err.Error())
			return err
		}
	}
	return nil
}

func uploadFiles(storageClient *storage.Client, dataSetName string, avroFiles []string) error {
	// UPLOAD FILES TO BUCKET
	for _, f := range avroFiles {
		err := gcp.UploadBlobToStorage(storageClient, data.BucketName, dataSetName, f)
		if err != nil {
			log.Printf("ERROR UPLOADING AVRO FILE: %v %v", f, err.Error())
			return err
		}
	}
	return nil
}

func writeRecordsToFile(formattedData []map[string]interface{}, dataSetName, jsonFile string) error {
	// DUMP THE RAW JSON TOO
	jsonData, err := json.Marshal(formattedData)
	if err != nil {
		return err
	}
	// WRITE THE JSON TO A FILE
	err = ioutil.WriteFile(fmt.Sprintf("%v/%v", dataSetName, jsonFile), jsonData, 0644)
	if err != nil {
		log.Printf("ERROR DUMPING FORMATTED JSON TO FILE: %v", err.Error())
		return err
	}
	return nil
}
