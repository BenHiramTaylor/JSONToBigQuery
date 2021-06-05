package avro

import (
	"encoding/json"
	"io"
	"log"
	"reflect"
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

func (s *Schema) ToJSON() ([]byte, error) {
	return json.Marshal(s)
}

func (s *Schema) FromJSON(fileReader io.Reader) error {
	return json.NewDecoder(fileReader).Decode(&s)
}
