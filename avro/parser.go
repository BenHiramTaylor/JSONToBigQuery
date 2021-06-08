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
)

func ParseRequest(request *data.JtBRequest) (ParsableSchema, []map[string]interface{}, error) {
	// GENERATE VARS
	var (
		parseWg    sync.WaitGroup
		formWg     sync.WaitGroup
		ParsedRecs []map[string]interface{}
	)
	fChan := make(chan map[string]interface{})
	rawChan := make(chan map[string]interface{})

	// GENERATE SCHEMA NAMES
	avroName := fmt.Sprintf("%v", request.TableName)
	avroNameSpace := fmt.Sprintf("%v.avsc", avroName)
	schema := NewSchema(avroName, avroNameSpace)
	pSchema := NewParsableSchema("array", *schema)

	// TRY TO LOAD AVSC FILE
	avscData, err := ioutil.ReadFile(fmt.Sprintf("%v/%v.avsc", request.DatasetName, request.TableName))
	if err != nil {
		if _, ok := err.(*os.PathError); !ok {
			log.Printf("ERROR READING AVSC FILE: %v", err.Error())
			return ParsableSchema{}, nil, err
		}
	} else {
		err = json.Unmarshal(avscData, pSchema)
		if err != nil {
			log.Printf("ERROR READING AVSC BYTES TO STRUCT: %v", err.Error())
			return ParsableSchema{}, nil, err
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
				ParseRecord(rec, "", formattedRec, fChan)
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
	parseWg.Wait()
	close(fChan)
	formWg.Wait()

	// ADD THE SLICE OF FORMATTED RECORDS TO THE SCHEMA STRUCT FOR EASIER METHOD ACCESS LATER
	log.Println("Finished parsing all records.")
	schema.GenerateSchemaFields(ParsedRecs)
	ParsedRecsWithNulls := schema.AddNulls(ParsedRecs)
	log.Printf("%v", ParsedRecsWithNulls)
	log.Printf("%#v", pSchema)
	return *pSchema, ParsedRecsWithNulls, nil
}
func ParseRecord(rec map[string]interface{}, fullKey string, formattedRec map[string]interface{}, fChan chan<- map[string]interface{}) {
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
			ParseRecord(v.(map[string]interface{}), k, formattedRec, fChan)
			// SET TO FALSE TO AVOID DUPLICATING RECORD FOR EACH NESTED DIC
			sendOnChan = false
		// IF IT IS AN ARRAY THEN PARSE IT INTO THE LIST MAPPINGS SCHEMA
		case reflect.Array:
			// TODO ADD LIST MAPPINGS LOGIC HERE
		default:
			formattedRec[k] = v
		}
	}
	// SEND THE FORMATTED RECORD OVER THE CHANNEL TO BE APPENDED TO THE FORMATTED LIST
	if sendOnChan {
		fChan <- formattedRec
	}
}
