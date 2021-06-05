package avro

import (
	"fmt"
	"log"
	"reflect"
	"sync"

	"github.com/BenHiramTaylor/JSONToBigQuery/data"
)

func ParseRequest(request *data.JtBRequest) (Schema, error) {
	// GENERATE VARS
	var (
		parseWg          sync.WaitGroup
		formWg           sync.WaitGroup
		mx               sync.Mutex
		FormattedRecords []map[string]interface{}
	)
	fChan := make(chan map[string]interface{})
	rawChan := make(chan map[string]interface{})

	// GENERATE SCHEMA NAMES
	avroName := fmt.Sprintf("%v.%v", request.DatasetName, request.TableName)
	avroNameSpace := fmt.Sprintf("%v.avsc", avroName)
	schema := NewSchema(avroName, avroNameSpace)

	log.Printf("Starting to parse %v records", len(request.Data))
	// GOROUTINE FOR ADDING FORMATTED RECS TO STRING
	for i := 0; i < 2; i++ {
		formWg.Add(1)
		go func() {
			defer formWg.Done()
			for rec := range fChan {
				mx.Lock()
				FormattedRecords = append(FormattedRecords, rec)
				mx.Unlock()
			}
		}()
	}
	for i := 0; i < 100; i++ {
		parseWg.Add(1)
		go func() {
			defer parseWg.Done()
			for rec := range rawChan {
				log.Printf("Formatting record: %v", rec)
				formattedRec := make(map[string]interface{})
				ParseRecord(rec, "", formattedRec, fChan)
			}
		}()
	}
	// ITERATE OVER RECS AND SEND THEM ON CHAN
	for _, v := range request.Data {
		rawChan <- v
	}
	// CLOSE CHANNEL OF RAW, WAIT FOR FORMATTING TO FINSIH, THEN CLOSE FORMATTING CHANNEL AND WAIT
	// FOR THAT GO ROUTINE TO COMPLETE ADDING TO LIST
	close(rawChan)
	parseWg.Wait()
	close(fChan)
	formWg.Wait()

	// ADD THE SLICE OF FORMATTED RECORDS TO THE SCHEMA STRUCT FOR EASIER METHOD ACCESS LATER
	schema.FormattedRecords = FormattedRecords
	log.Println("Finished parsing all records.")
	log.Printf("%v", schema.FormattedRecords)
	log.Println("Generating Schema.")
	schema.GenerateSchemaFields()
	schema.EqualiseData()
	log.Printf("%#v", schema)
	return *schema, nil
}
func ParseRecord(rec map[string]interface{}, fullKey string, formattedRec map[string]interface{}, fChan chan<- map[string]interface{}) {
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
			ParseRecord(v.(map[string]interface{}), k, formattedRec, fChan)
		// IF IT IS AN ARRAY THEN PARSE IT INTO THE LIST MAPPINGS SCHEMA
		case reflect.Array:
			// TODO ADD LIST MAPPINGS LOGIC HERE
		// IF IT IS STRING THEN ADD IT TO THE EMPTY OVERALL RECORD AND SCHEMA AS STRING
		case reflect.String:
			formattedRec[k] = v
		// ELSE ADD IT AS TYPE INT
		default:
			formattedRec[k] = v
		}
	}
	// SEND THE FORMATTED RECORD OVER THE CHANNEL TO BE APPENDED TO THE FORMATTED LIST
	fChan <- formattedRec
}
