package avro

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"reflect"
	"sync"

	"github.com/BenHiramTaylor/JSONToBigQuery/data"
	"github.com/BenHiramTaylor/JSONToBigQuery/gcp"
)

func parseListMappings(request *data.JtBRequest, listChan <-chan map[string]interface{}) {
	var (
		storWg       sync.WaitGroup
		listMappings = make([]map[string]interface{}, len(listChan))
		listSchema   = Schema{
			Name:      fmt.Sprintf("%v.ListMappings.avro", request.TableName),
			Namespace: fmt.Sprintf("%v.ListMappings.avsc", request.TableName),
			Type:      "record",
			Fields: []Field{
				{Name: "tableName", FieldType: []string{"string", "null"}},
				{Name: "idField", FieldType: []string{"string", "null"}},
				{Name: "Key", FieldType: []string{"string", "null"}},
				{Name: "Value", FieldType: []string{"string", "null"}},
			},
		}
	)
	log.Printf("LIST SCHEMA: %#v", listSchema)
	for m := range listChan {
		for k, v := range m {
			for _, lv := range v.([]interface{}) {
				listMappings = append(listMappings, map[string]interface{}{"tableName": request.TableName, "idField": request.IdField, "Key": k, "Value": lv})
			}
		}
	}
	log.Printf("Finished Parsing all list mappings: %v", listMappings)
	// PARSE OUR AVSC DATA THROUGH THE ENCODER
	avroBytes, err := listSchema.WriteRecords(listMappings)
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

	storWg.Add(1)
	go func() {
		// CREATE A STORAGE CLIENT TO TEST THE AUTH
		storClient, err := gcp.GetStorageClient(data.CredsFilePath)
		if err != nil {
			log.Printf("ERROR CREATING GCS CLIENT: %v", err.Error())
			return
		}
		if storClient == nil {
			log.Printf("ERROR CREATING GCS CLIENT: CLIENT IS NIL IN LISTMAPPINGS")
			return
		}

		// UPLOAD FILE TO BUCKET
		err = gcp.UploadBlobToStorage(storClient, data.BucketName, request.DatasetName, listSchema.Name)
		if err != nil {
			log.Printf("ERROR UPLOADING AVRO FILE: %v %v", listSchema.Name, err.Error())
		}
		storWg.Done()
	}()

	// CREATING BQ CLIENT
	bqClient, err := gcp.GetBQClient(data.CredsFilePath, request.ProjectID)
	if err != nil {
		log.Printf("ERROR CREATING BQ CLIENT: %v", err.Error())
		return
	}
	if bqClient == nil {
		log.Printf("ERROR CREATING BQ CLIENT: CLIENT IS NIL IN LISTMAPPINGS")
		return
	}

	storWg.Wait()
	// CREATE TABLE AND ADD ANY NEW SCHEMA USING SCHEMA FIELD NAMES
	err = gcp.PrepareTable(bqClient, request.DatasetName, "ListMappings", []string{}, listSchema)
	if err != nil {
		log.Println("ERROR PREPARING TABLE: ListMappings")
		return
	}
	// LOAD THE DATA FROM GCS
	err = gcp.LoadAvroToTable(bqClient, data.BucketName, request.DatasetName, "ListMappings", listSchema.Name)
	if err != nil {
		log.Printf("ERROR LOADING LISTMAPPINGS TABLE: %v", err.Error())
		return
	}
}

func ParseRequest(request *data.JtBRequest) (Schema, []map[string]interface{}, []string, error) {
	// GENERATE VARS
	var (
		parseWg    sync.WaitGroup
		formWg     sync.WaitGroup
		ParsedRecs []map[string]interface{}
		listChan   = make(chan map[string]interface{})
		fChan      = make(chan map[string]interface{})
		rawChan    = make(chan map[string]interface{})
	)

	// GENERATE SCHEMA NAMES
	avroName := fmt.Sprintf("%v", request.TableName)
	avroNameSpace := fmt.Sprintf("%v.avsc", avroName)
	schema := NewSchema(avroName, avroNameSpace)

	// START GOROUTINE FOR PARSING LIST MAPPINGS
	parseWg.Add(1)
	go func() {
		parseListMappings(request, listChan)
		parseWg.Done()
	}()

	// TRY TO LOAD AVSC FILE
	avscData, err := ioutil.ReadFile(fmt.Sprintf("%v/%v.avsc", request.DatasetName, request.TableName))
	if err != nil {
		if _, ok := err.(*os.PathError); !ok {
			log.Printf("ERROR READING AVSC FILE: %v", err.Error())
			return Schema{}, nil, nil, err
		}
	} else {
		err = json.Unmarshal(avscData, schema)
		if err != nil {
			log.Printf("ERROR READING AVSC BYTES TO STRUCT: %v", err.Error())
			return Schema{}, nil, nil, err
		} else {
			log.Printf("LOADED SCHEMA FROM GCS: %#v", schema)
		}
	}

	log.Printf("Starting to parse %v records", len(request.Data))
	// GOROUTINE FOR ADDING FORMATTED RECS TO STRING
	formWg.Add(1)
	go func() {
		defer formWg.Done()
		for rec := range fChan {
			ParsedRecs = append(ParsedRecs, rec)
		}
	}()

	for i := 0; i < 100; i++ {
		parseWg.Add(1)
		go func() {
			defer parseWg.Done()
			for rec := range rawChan {
				formattedRec := make(map[string]interface{})
				ParseRecord(rec, "", formattedRec, fChan, listChan)
			}
		}()
	}
	// ITERATE OVER RECS AND SEND THEM ON CHAN
	for _, v := range request.Data {
		rawChan <- v
	}
	// CLOSE CHANNEL OF RAW, WAIT FOR FORMATTING TO FINISH, THEN CLOSE FORMATTING CHANNEL AND WAIT
	// FOR THAT GO ROUTINE TO COMPLETE ADDING TO LIST
	close(rawChan)
	close(listChan)
	parseWg.Wait()
	close(fChan)
	formWg.Wait()

	// ADD THE SLICE OF FORMATTED RECORDS TO THE SCHEMA STRUCT FOR EASIER METHOD ACCESS LATER
	log.Println("Finished parsing all records.")
	timestampFields := schema.GenerateSchemaFields(ParsedRecs, request.TimestampFormat)
	ParsedRecsWithNulls := schema.AddNulls(ParsedRecs)
	log.Printf("PARSED RECS WITH NULLS: %v", ParsedRecsWithNulls)
	log.Printf("FULL SCHEMA: %#v", schema)
	return *schema, ParsedRecsWithNulls, timestampFields, nil
}
func ParseRecord(rec map[string]interface{}, fullKey string, formattedRec map[string]interface{}, fChan chan<- map[string]interface{}, listChan chan<- map[string]interface{}) {
	sendOnChan := true
	// FOR KEY VAL IN THE JSON BLOB
	for k, v := range rec {
		// IF KEY IS PART OF NESTED DIC, COMBINE THE KEYS
		if fullKey != "" {
			k = fmt.Sprintf("%v_%v", fullKey, k)
		}
		// BEGIN SWITCH STATEMENT FOR THE TYPE OF VALUE
		switch reflect.ValueOf(v).Kind() {
		// IF ITS ANOTHER DICT THEN RECURSIVLY REPEAT TO FLATTEN OUT STRUCTURE
		case reflect.Map:
			ParseRecord(v.(map[string]interface{}), k, formattedRec, fChan, listChan)
			// SET TO FALSE TO AVOID DUPLICATING RECORD FOR EACH NESTED DIC
			sendOnChan = false
		// IF IT IS AN ARRAY THEN PARSE IT INTO THE LIST MAPPINGS SCHEMA
		case reflect.Array:
			listChan <- map[string]interface{}{k: v}
		default:
			formattedRec[k] = v
		}
	}
	// SEND THE FORMATTED RECORD OVER THE CHANNEL TO BE APPENDED TO THE FORMATTED LIST
	if sendOnChan {
		fChan <- formattedRec
	}
}
