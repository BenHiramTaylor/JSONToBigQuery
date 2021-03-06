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

// ParseRequest Parses the request object, maps schema, returns formatted
// records, a slice of all the timestamp fields and listMappings
func ParseRequest(request *data.JTBRequest) (Schema, []map[string]interface{}, []map[string]interface{}, []string, error) {
	// GENERATE VARS
	var (
		parseWg      sync.WaitGroup
		formWg       sync.WaitGroup
		listWg       sync.WaitGroup
		ParsedRecs   []map[string]interface{}
		ListMappings []map[string]interface{}
		fChan        = make(chan map[string]interface{})
		rawChan      = make(chan map[string]interface{})
		listChan     = make(chan map[string]interface{})
	)

	// GENERATE SCHEMA NAMES
	avroName := fmt.Sprintf("%v", request.TableName)
	avroNameSpace := fmt.Sprintf("%v.avsc", avroName)
	schema := NewSchema(avroName, avroNameSpace)

	// TRY TO LOAD AVSC FILE
	avscData, err := ioutil.ReadFile(fmt.Sprintf("%v/%v.avsc", request.DatasetName, request.TableName))
	if err != nil {
		if _, ok := err.(*os.PathError); !ok {
			log.Printf("ERROR READING AVSC FILE: %v", err.Error())
			return Schema{}, nil, nil, nil, err
		}
	} else {
		err = json.Unmarshal(avscData, schema)
		if err != nil {
			log.Printf("ERROR READING AVSC BYTES TO STRUCT: %v", err.Error())
			return Schema{}, nil, nil, nil, err
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
	// GOROUTINE FOR ADDING LIST MAPPINGS
	listWg.Add(1)
	go func() {
		defer listWg.Done()
		for rec := range listChan {
			ListMappings = append(ListMappings, rec)
		}
	}()

	for i := 0; i < 100; i++ {
		parseWg.Add(1)
		go func() {
			defer parseWg.Done()
			for rec := range rawChan {
				idField := rec[request.IdField]
				formattedRec := make(map[string]interface{})
				ParseRecord(rec, "", formattedRec, fChan, request.TableName, fmt.Sprintf("%v", idField), listChan)
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
	close(listChan)
	listWg.Wait()
	close(fChan)
	formWg.Wait()

	// ADD THE SLICE OF FORMATTED RECORDS TO THE SCHEMA STRUCT FOR EASIER METHOD ACCESS LATER
	log.Println("Finished parsing all records.")
	timestampFields := schema.GenerateSchemaFields(ParsedRecs, request.TimestampFormat)
	ParsedRecsWithNulls := schema.AddNulls(ParsedRecs)
	log.Printf("PARSED RECS WITH NULLS: %v", ParsedRecsWithNulls)
	log.Printf("FULL SCHEMA: %#v", schema)
	return *schema, ParsedRecsWithNulls, ListMappings, timestampFields, nil
}

// ParseRecord Recursivly parses a record flattening nested dics and parsing out
// all lists
func ParseRecord(rec map[string]interface{}, fullKey string, formattedRec map[string]interface{}, fChan chan<- map[string]interface{}, TableName, IdField string, ListMapChan chan<- map[string]interface{}) {
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
			ParseRecord(v.(map[string]interface{}), k, formattedRec, fChan, TableName, IdField, ListMapChan)
			// SET TO FALSE TO AVOID DUPLICATING RECORD FOR EACH NESTED DIC
			sendOnChan = false
		// IF IT IS AN ARRAY THEN PARSE IT INTO THE LIST MAPPINGS SCHEMA
		case reflect.Array, reflect.Slice:
			for _, lv := range v.([]interface{}) {
				ListMapChan <- map[string]interface{}{"tableName": TableName, "idField": IdField, "Key": k, "Value": lv}
			}

		default:
			formattedRec[k] = v
		}
	}
	// SEND THE FORMATTED RECORD OVER THE CHANNEL TO BE APPENDED TO THE FORMATTED LIST
	if sendOnChan {
		fChan <- formattedRec
	}
}
