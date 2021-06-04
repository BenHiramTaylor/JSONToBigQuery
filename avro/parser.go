package avro

import (
	"fmt"
	"log"
	"reflect"
	"sync"

	"github.com/BenHiramTaylor/JSONToBigQuery/data"
)

func ParseRequest(request *data.JtBRequest) (Schema, error) {
	var wg sync.WaitGroup
	avroName := fmt.Sprintf("%v.%v", request.DatasetName, request.TableName)
	avroNameSpace := fmt.Sprintf("%v.avsc", avroName)
	schema := NewSchema(avroName, avroNameSpace)
	log.Printf("Starting to parse %v records", len(request.Data))
	for _, v := range request.Data {
		wg.Add(1)
		go func() {
			schema.ParseRecord(v, "")
			wg.Done()
		}()
	}
	wg.Wait()
	log.Println("Finished parsing all records.")
	log.Printf("%#v", schema)
	return *schema, nil
}
func (s *Schema) ParseRecord(rec map[string]interface{}, fullKey string) map[string]interface{} {
	for k, v := range rec {
		if fullKey != "" {
			k = fmt.Sprintf("%v_%v", fullKey, k)
		}
		switch reflect.ValueOf(v).Kind() {
		case reflect.Map:
			rec[k] = s.ParseRecord(v.(map[string]interface{}), k)
		case reflect.Array:
			// TODO ADD LIST MAPPINGS LOGIC HERE
		case reflect.String:
			s.AddField(k, "string")
		default:
			s.AddField(k, "int")
		}
		log.Printf("Parsing record: Key: %v Value: %v", k, v)
	}
	return rec
}
