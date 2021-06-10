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

type JSONFormattedData []map[string]interface{}

type Field struct {
	Name      string   `json:"name"`
	FieldType []string `json:"type"`
}

type Schema struct {
	Type      string  `json:"type"`
	Name      string  `json:"name"`
	Namespace string  `json:"namespace"`
	Fields    []Field `json:"fields"`
}

func NewSchema(name string, namespace string) *Schema {
	return &Schema{Type: "record", Name: name, Namespace: namespace}
}

func NewField(name string, fieldType string) *Field {
	arrFieldType := []string{fieldType, "null"}
	return &Field{Name: name, FieldType: arrFieldType}
}

func (s *Schema) AddField(FieldName, Type string) {
	for i, f := range s.Fields {
		if f.Name == FieldName {
			for _, fv := range f.FieldType {
				if fv == "null" {
					continue
				}
				if fv != Type {
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

func isFloatInt(floatValue float64) bool {
	return math.Mod(floatValue, 1.0) == 0
}

func (s *Schema) GenerateSchemaFields(FormattedRecords []map[string]interface{}) []string {
	timestampFields := make([]string, len(FormattedRecords))
	for _, rec := range FormattedRecords {
		for k, v := range rec {
			switch reflect.ValueOf(v).Kind() {
			case reflect.Int64:
				s.AddField(k, "long")
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32:
				s.AddField(k, "int")
			case reflect.Bool:
				s.AddField(k, "boolean")
			case reflect.Float32:
				s.AddField(k, "float")
			case reflect.Float64:
				// CHECK IF FLOAT IS ACTUALLY AN INT BECAUSE JSON UNMARSHALLS ALL NUMBERS AS FLOAT64
				// IF IT IS, EDIT THE VALUE SO IT IS AN INT AND THEN USE INT SCHEMA
				isInt := isFloatInt(v.(float64))
				if !isInt {
					s.AddField(k, "double")
				} else {
					newV := int(v.(float64))
					rec[k] = newV
					s.AddField(k, "int")
				}
			case reflect.String:
				// ATTEMPT TO CONVERT STRINGS TO time.Time objects, IF IT FAILS THEN ITS JUST STRING, ELSE MAKE IT A TIMESTAMP
				newV, _ := v.(string)
				timeVal, err := time.Parse(time.RFC3339, newV)
				if err == nil {
					// BIGQUERY TAKES UNIX MICROS SO WE GET NANO AND DIVIDE BY 1000
					rec[k] = timeVal.UnixNano() / 1000
					timestampFields = append(timestampFields, k)
					s.AddField(k, "long")
				} else {
					s.AddField(k, "string")
				}
			}
		}
	}
	return timestampFields
}

func (s *Schema) AddNulls(FormattedRecords []map[string]interface{}) []map[string]interface{} {
	var (
		rawWg                 sync.WaitGroup
		mx                    sync.Mutex
		rChan                 = make(chan map[string]interface{}, len(FormattedRecords))
		FormattedRecordsNulls []map[string]interface{}
	)
	for i := 0; i < 100; i++ {
		rawWg.Add(1)
		go func() {
			defer rawWg.Done()
			for rec := range rChan {
				for _, f := range s.Fields {
					exists := false
					for k := range rec {
						// IF SCHEMA KEY IS IN RECORD THEN BREAK, ELSE KEEP LOOKING IN REC
						if k == f.Name {
							exists = true
							break
						} else {
							continue
						}
					}
					if !exists {
						rec[f.Name] = nil
					}
				}
				mx.Lock()
				FormattedRecordsNulls = append(FormattedRecordsNulls, rec)
				mx.Unlock()
			}
		}()
	}
	for _, v := range FormattedRecords {
		rChan <- v
	}
	close(rChan)
	rawWg.Wait()
	log.Println("Added all nulls to data.")
	return FormattedRecordsNulls
}

func (s *Schema) ToJSON() ([]byte, error) {
	return json.Marshal(s)
}

func (s *Schema) FromJSON(fileReader io.Reader) error {
	return json.NewDecoder(fileReader).Decode(&s)
}

func (s *Schema) ToFile(dataset string) error {
	JSONb, err := s.ToJSON()
	if err != nil {
		return err
	}
	err = ioutil.WriteFile(fmt.Sprintf("%v/%v", dataset, s.Namespace), JSONb, 0644)
	if err != nil {
		return err
	}
	return nil
}

func (s *Schema) WriteRecords(records []map[string]interface{}) ([]byte, error) {
	w := &bytes.Buffer{}
	schemaBytes, err := s.ToJSON()
	if err != nil {
		return nil, err
	}
	ocf.WithCodec(ocf.Snappy)
	enc, err := ocf.NewEncoder(string(schemaBytes), w)
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
	return w.Bytes(), nil
}
