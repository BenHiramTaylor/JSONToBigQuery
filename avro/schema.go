package avro

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math"
	"reflect"
	"sync"
	"time"
)

type JSONFormattedData []map[string]interface{}

type Field struct {
	Name      string   `json:"name"`
	FieldType []string `json:"type"`
}

type RowSchema struct {
	Type      string  `json:"type"`
	Name      string  `json:"name"`
	Namespace string  `json:"namespace"`
	Fields    []Field `json:"fields"`
}

type Schema struct {
	Type  string    `json:"type"`
	Items RowSchema `json:"items"`
}

func NewParsableSchema(Type string, items RowSchema) *Schema {
	return &Schema{Type: Type, Items: items}
}

func NewSchema(name string, namespace string) *RowSchema {
	return &RowSchema{Type: "record", Name: name, Namespace: namespace}
}

func NewField(name string, fieldType string) *Field {
	arrFieldType := []string{fieldType, "null"}
	return &Field{Name: name, FieldType: arrFieldType}
}

func (r *RowSchema) AddField(FieldName, Type string) {
	for i, f := range r.Fields {
		if f.Name == FieldName {
			for _, fv := range f.FieldType {
				if fv == "null" {
					continue
				}
				if fv != Type {
					r.Fields[i] = *NewField(FieldName, "string")
					return
				} else {
					return
				}
			}
		}
	}

	nf := NewField(FieldName, Type)
	log.Printf("New Field Added to %v : %#v", r.Namespace, nf)
	r.Fields = append(r.Fields, *nf)
}

func isFloatInt(floatValue float64) bool {
	return math.Mod(floatValue, 1.0) == 0
}

func (r *RowSchema) GenerateSchemaFields(FormattedRecords []map[string]interface{}) {
	for _, rec := range FormattedRecords {
		for k, v := range rec {
			switch reflect.ValueOf(v).Kind() {
			case reflect.String:
				// ATTEMPT TO CONVERT STRINGS TO time.Time objects, IF IT FAILS THEN ITS JUST STRING, ELSE MAKE IT A TIMESTAMP
				newV, _ := v.(string)
				timeVal, err := time.Parse(time.RFC3339, newV)
				if err == nil {
					rec[k] = timeVal.UnixNano()
					r.AddField(k, "long.timestamp-micros")
				}
				r.AddField(k, "string")
			case reflect.Int64:
				r.AddField(k, "long")
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32:
				r.AddField(k, "int")
			case reflect.Bool:
				r.AddField(k, "boolean")
			case reflect.Float32:
				r.AddField(k, "float")
			case reflect.Float64:
				// CHECK IF FLOAT IS ACTUALLY AN INT BECAUSE JSON UNMARSHALLS ALL NUMBERS AS FLOAT64
				// IF IT IS, EDIT THE VALUE SO IT IS AN INT AND THEN USE INT SCHEMA
				isInt := isFloatInt(v.(float64))
				if !isInt {
					r.AddField(k, "double")
				} else {
					newV, _ := v.(int)
					rec[k] = newV
					r.AddField(k, "int")
				}
			default:
				r.AddField(k, "string")
			}
		}
	}
}

func (r *RowSchema) AddNulls(FormattedRecords []map[string]interface{}) []map[string]interface{} {
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
				for _, f := range r.Fields {
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

func (r *RowSchema) ToJSON() ([]byte, error) {
	return json.Marshal(r)
}

func (s *Schema) ToJSON() ([]byte, error) {
	return json.Marshal(s)
}

func (r *RowSchema) FromJSON(fileReader io.Reader) error {
	return json.NewDecoder(fileReader).Decode(&r)
}

func (r *RowSchema) ToFile(dataset string) error {
	JSONb, err := r.ToJSON()
	if err != nil {
		return err
	}
	err = ioutil.WriteFile(fmt.Sprintf("%v/%v", dataset, r.Namespace), JSONb, 0644)
	if err != nil {
		return err
	}
	return nil
}
func (s *Schema) ToFile(dataset string) error {
	JSONb, err := s.ToJSON()
	if err != nil {
		return err
	}
	err = ioutil.WriteFile(fmt.Sprintf("%v/%v", dataset, s.Items.Namespace), JSONb, 0644)
	if err != nil {
		return err
	}
	return nil
}
