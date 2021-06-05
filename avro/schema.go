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

type FieldType []string

type Field struct {
	Name      string    `json:"name"`
	FieldType FieldType `json:"type"`
}

type Schema struct {
	Type      string  `json:"type"`
	Name      string  `json:"name"`
	Namespace string  `json:"namespace"`
	Fields    []Field `json:"fields"`
	// CHANGE JSON TAG TO - WHEN DONE TO PREVENT EXPORTING TO AVRO FILE
	FormattedRecords []map[string]interface{} `json:"FormattedRecords"`
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

func (s *Schema) GenerateSchemaFields() {
	for _, rec := range s.FormattedRecords {
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

// TODO SORT THIS OUT LATER
func (s *Schema) EqualiseData() {
	var (
		rawWg   sync.WaitGroup
		eqWg    sync.WaitGroup
		mx      sync.Mutex
		fChan   chan map[string]interface{}
		eqSlice []map[string]interface{}
	)
	for i := 0; i < 5; i++ {
		eqWg.Add(1)
		go func() {
			defer eqWg.Done()
			for rec := range fChan {
				mx.Lock()
				eqSlice = append(eqSlice, rec)
				mx.Unlock()
			}
		}()
	}
	for _, rec := range s.FormattedRecords {
		rawWg.Add(1)
		go func() {
			defer rawWg.Done()
			tempMap := make(map[string]interface{})
			for _, f := range s.Fields {
				for k, v := range rec {
					// IF SCHEMA KEY IS IN RECORD THEN BREAK, ELSE KEEP LOOKING IN REC
					if k == f.Name {
						tempMap[k] = v
						break
					} else if k != f.Name {
						tempMap[k] = v
						continue
					}

				}
				tempMap[f.Name] = nil
			}
			fChan <- tempMap
		}()
	}
	rawWg.Wait()
	close(fChan)
	eqWg.Wait()
	log.Println("Equalised all data.")
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
