package avro

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"reflect"
	"sync"
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

func (s *Schema) GenerateSchemaFields(FormattedRecords []map[string]interface{}) {
	for _, rec := range FormattedRecords {
		for k, v := range rec {
			switch reflect.ValueOf(v).Kind() {
			case reflect.String:
				s.AddField(k, "string")
			case reflect.Int64, reflect.Uint64:
				s.AddField(k, "long")
			case reflect.Bool:
				s.AddField(k, "boolean")
			default:
				s.AddField(k, "int")
			}
		}
	}
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
