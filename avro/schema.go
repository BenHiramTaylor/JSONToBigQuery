package avro

import (
	"encoding/json"
	"io"
)

type AvroField struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

type AvroSchema struct {
	Type      string      `json:"type"`
	Name      string      `json:"name"`
	Namespace string      `json:"namespace"`
	Fields    []AvroField `json:"fields"`
}

func NewAvroSchema(avroType string, name string, namespace string) *AvroSchema {
	return &AvroSchema{Type: avroType, Name: name, Namespace: namespace}
}

func (a *AvroSchema) ToJSON() ([]byte, error) {
	return json.Marshal(a)
}

func (a *AvroSchema) FromJSON(fileReader io.Reader) error {
	return json.NewDecoder(fileReader).Decode(&a)
}
