package avro

import (
	"encoding/json"
	"io"
	"log"
)

var (
	KeyList          []string
	FormattedRecords []map[string]interface{}
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
}

func NewSchema(name string, namespace string) *Schema {
	return &Schema{Type: "record", Name: name, Namespace: namespace}
}

func NewField(name string, fieldType string) *Field {
	arrFieldType := []string{fieldType, "null"}
	return &Field{Name: name, FieldType: arrFieldType}
}

func (s *Schema) AddField(FieldName, Type string) {
	// TODO NEED TO REFACTOR THIS TO USE EITHER MUTEX OR CHANNELS
	for i, f := range s.Fields {
		if f.Name == FieldName {
			log.Printf("Field %v already exists in Schema: %v", FieldName, s.Namespace)
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

func (s *Schema) ToJSON() ([]byte, error) {
	return json.Marshal(s)
}

func (s *Schema) FromJSON(fileReader io.Reader) error {
	return json.NewDecoder(fileReader).Decode(&s)
}
