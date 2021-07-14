package avro

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math"
	"reflect"
	"sync"
	"time"

	"github.com/hamba/avro/ocf"
)

// Field a field object, includes name of field and type of field
type Field struct {
	Name      string   `json:"name"`
	FieldType []string `json:"type"`
}

// Schema a schema obejct, represents avro schema
type Schema struct {
	Type      string  `json:"type"`
	Name      string  `json:"name"`
	Namespace string  `json:"namespace"`
	Fields    []Field `json:"fields"`
}

// NewSchema Returns a blank schema object
func NewSchema(name string, namespace string) *Schema {
	return &Schema{Type: "record", Name: name, Namespace: namespace}
}

// NewField Creates a new field object and returns it, used for overwriting
func NewField(name string, fieldType string) *Field {
	arrayFieldType := []string{fieldType, "null"}
	return &Field{Name: name, FieldType: arrayFieldType}
}

// AddField Adds a field to the schema, allowing it to be nulled
func (s *Schema) AddField(FieldName, Type string) {
	for i, field := range s.Fields {
		if field.Name == FieldName {
			for _, fieldValue := range field.FieldType {
				if fieldValue == "null" {
					continue
				}
				if fieldValue != Type {
					s.Fields[i] = *NewField(FieldName, "string")
					return
				} else {
					return
				}
			}
		}
	}

	nf := NewField(FieldName, Type)
	log.Printf("New Field Added to %v : %#v", s.Namespace, nf)
	s.Fields = append(s.Fields, *nf)
}

// CHECKS IF A FLOAT IS AN INTEGER, THIS IS BECAUSE THE GOLAND
// JSON PACKAGE UNMARSHALS ALL INTEGERS AS A FLOAT TYPE, EVEN IF THE VALUE IS A WHOLE NUMBER
func isFloatInt(floatValue float64) bool {
	return math.Mod(floatValue, 1.0) == 0
}

// GenerateSchemaFields Iterates over the records and generates schema, this
// also ensures that schema is up to date if new cols are added to the data
func (s *Schema) GenerateSchemaFields(FormattedRecords []map[string]interface{}, timestampFormat string) []string {
	log.Printf("GOT TIMESTAMP FORMAT: %v", timestampFormat)
	timestampFields := make([]string, len(FormattedRecords))
	for _, record := range FormattedRecords {
		for recordKey, recordValue := range record {
			switch reflect.ValueOf(recordValue).Kind() {
			case reflect.Int64:
				s.AddField(recordKey, "long")
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32:
				s.AddField(recordKey, "int")
			case reflect.Bool:
				s.AddField(recordKey, "boolean")
			case reflect.Float32:
				s.AddField(recordKey, "float")
			case reflect.Float64:
				// CHECK IF FLOAT IS ACTUALLY AN INT BECAUSE JSON UNMARSHALLS ALL NUMBERS AS FLOAT64
				// IF IT IS, EDIT THE VALUE SO IT IS AN INT AND THEN USE INT SCHEMA
				isInt := isFloatInt(recordValue.(float64))
				if !isInt {
					s.AddField(recordKey, "double")
				} else {
					newValue := int(recordValue.(float64))
					record[recordKey] = newValue
					s.AddField(recordKey, "int")
				}
			case reflect.String:
				// ATTEMPT TO CONVERT STRINGS TO time.Time objects, IF IT FAILS THEN ITS JUST STRING, ELSE MAKE IT A TIMESTAMP
				newValue, _ := recordValue.(string)
				timeValue, err := time.Parse(timestampFormat, newValue)
				if err == nil {
					// BIGQUERY TAKES UNIX MICROS SO WE GET NANO AND DIVIDE BY 1000
					record[recordKey] = timeValue.UnixNano() / 1000
					timestampFields = append(timestampFields, recordKey)
					s.AddField(recordKey, "long")
				} else {
					s.AddField(recordKey, "string")
				}
			}
		}
	}
	return timestampFields
}

// AddNulls This function will add nulls of the missing values that are in the
// schema but not in the data this is because avro cant handle it without the
// schema matching each object
func (s *Schema) AddNulls(FormattedRecords []map[string]interface{}) []map[string]interface{} {
	var (
		rawRecordWaitGroup    sync.WaitGroup
		mutex                 sync.Mutex
		resultsChan           = make(chan map[string]interface{}, len(FormattedRecords))
		FormattedRecordsNulls []map[string]interface{}
	)
	for i := 0; i < 100; i++ {
		rawRecordWaitGroup.Add(1)
		go func() {
			defer rawRecordWaitGroup.Done()
			for record := range resultsChan {
				for _, field := range s.Fields {
					exists := false
					for recordKey := range record {
						// IF SCHEMA KEY IS IN RECORD THEN BREAK, ELSE KEEP LOOKING IN REC
						if recordKey == field.Name {
							exists = true
							break
						} else {
							continue
						}
					}
					if !exists {
						record[field.Name] = nil
					}
				}
				mutex.Lock()
				FormattedRecordsNulls = append(FormattedRecordsNulls, record)
				mutex.Unlock()
			}
		}()
	}
	for _, v := range FormattedRecords {
		resultsChan <- v
	}
	close(resultsChan)
	rawRecordWaitGroup.Wait()
	log.Println("Added all nulls to data.")
	return FormattedRecordsNulls
}

// ToJSON Returns the schema struct in a JSON byte slice
func (s *Schema) ToJSON() ([]byte, error) {
	return json.Marshal(s)
}

// FromJSON Loads a JSON file into the schema struct
func (s *Schema) FromJSON(fileReader io.Reader) error {
	return json.NewDecoder(fileReader).Decode(&s)
}

// ToFile Dumps the schema to json then writes that to a file
func (s *Schema) ToFile(dataset string) error {
	jsonBytes, err := s.ToJSON()
	if err != nil {
		return err
	}
	err = ioutil.WriteFile(fmt.Sprintf("%v/%v", dataset, s.Namespace), jsonBytes, 0644)
	if err != nil {
		return err
	}
	return nil
}

// WriteRecords This function writes the records passed to the an avro file
// using the schema.
func (s *Schema) WriteRecords(records []map[string]interface{}) ([]byte, error) {
	bytesBuffer := &bytes.Buffer{}
	schemaBytes, err := s.ToJSON()
	if err != nil {
		return nil, err
	}
	ocf.WithCodec(ocf.Snappy)
	enc, err := ocf.NewEncoder(string(schemaBytes), bytesBuffer)
	if err != nil {
		log.Printf("ERROR CREATING ENCODER: %v", err.Error())
		return nil, err
	}
	for _, v := range records {
		err = enc.Encode(v)
		if err != nil {
			return nil, err
		}
	}
	if err = enc.Flush(); err != nil {
		return nil, err
	}
	return bytesBuffer.Bytes(), nil
}
