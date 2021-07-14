package gcp

import (
	"context"
	"fmt"
	"log"

	"cloud.google.com/go/bigquery"
	"github.com/BenHiramTaylor/JSONToBigQuery/avro"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"
)

var (
	// Map of string field representations to bigquery field types
	bqSchemaMap = map[string]bigquery.FieldType{
		"string":  bigquery.StringFieldType,
		"int":     bigquery.IntegerFieldType,
		"long":    bigquery.IntegerFieldType,
		"float":   bigquery.FloatFieldType,
		"double":  bigquery.FloatFieldType,
		"boolean": bigquery.BooleanFieldType,
	}
)

// GetBQClient Constructor func returns a Bigquery Client
func GetBQClient(credsPath string, projectID string) (*bigquery.Client, error) {
	ctx := context.Background()
	client, err := bigquery.NewClient(ctx, projectID, option.WithCredentialsFile(credsPath))
	if err != nil {
		return nil, err
	}
	log.Println("Created BigQuery Client")
	return client, nil
}

// Takes schema and updates a table to ensure the schema is up to date
func updateTableSchema(client *bigquery.Client, datasetID, tableID string, timestampFields []string, sch avro.Schema) error {
	var newSchema = bigquery.Schema{}
	ctx := context.Background()
	tableRef := client.Dataset(datasetID).Table(tableID)
	tableMetadata, err := tableRef.Metadata(ctx)
	if err != nil {
		return err
	}
	newSchema = append(tableMetadata.Schema)
	for _, avroField := range sch.Fields {
		exists := false
		avroFieldType := ""
		for _, tableField := range newSchema {
			if avroField.Name == tableField.Name {
				exists = true
				break
			} else {
				continue
			}
		}
		if !exists {
			for _, v := range avroField.FieldType {
				if v == "null" {
					continue
				} else {
					avroFieldType = v
				}
			}
			newSchema = append(newSchema,
				&bigquery.FieldSchema{Name: avroField.Name, Type: bqSchemaMap[avroFieldType]},
			)
		}
	}
	for i, fieldSchema := range newSchema {
		for _, timestampKey := range timestampFields {
			if timestampKey != fieldSchema.Name {
				continue
			} else {
				newSchema[i] = &bigquery.FieldSchema{Name: fieldSchema.Name, Type: bigquery.TimestampFieldType}
			}
		}
	}

	update := bigquery.TableMetadataToUpdate{
		Schema: newSchema,
	}
	if _, err := tableRef.Update(ctx, update, tableMetadata.ETag); err != nil {
		return err
	}
	return nil
}

// Creates a dataset and then table if it doesnt already exist
func createTable(client *bigquery.Client, datasetID, tableID string) error {
	ctx := context.Background()
	err := client.Dataset(datasetID).Create(ctx, &bigquery.DatasetMetadata{Name: datasetID})
	if err != nil {
		if e, ok := err.(*googleapi.Error); ok {
			if e.Code != 409 {
				return err
			}
		}
	}
	err = client.Dataset(datasetID).Table(tableID).Create(ctx, &bigquery.TableMetadata{Name: tableID})
	if err != nil {
		if e, ok := err.(*googleapi.Error); ok {
			if e.Code != 409 {
				return err
			}
		}
	}
	return nil
}

// Function used only in this package, used to retunr the schema of a table
func getTableSchema(client *bigquery.Client, datasetID, tableID string) (bigquery.Schema, error) {
	ctx := context.Background()
	tableRef := client.Dataset(datasetID).Table(tableID)
	meta, err := tableRef.Metadata(ctx)
	if err != nil {
		return nil, err
	}
	return meta.Schema, nil
}

// PrepareTable Creates a table if it doesnt exist, then updates the schema to
// match the avro schema parsed in
func PrepareTable(client *bigquery.Client, datasetID, tableID string, timestampFields []string, sch avro.Schema) error {
	err := createTable(client, datasetID, tableID)
	if err != nil {
		return err
	}
	err = updateTableSchema(client, datasetID, tableID, timestampFields, sch)
	if err != nil {
		return err
	}
	return nil
}

// LoadAvroToTable Loads avro data into a BQ table from google cloud storage reference
func LoadAvroToTable(client *bigquery.Client, bucketName, datasetID, tableID, avroFile string) error {
	tableSchema, err := getTableSchema(client, datasetID, tableID)
	if err != nil {
		return err
	}
	ctx := context.Background()
	gcsRef := bigquery.NewGCSReference(fmt.Sprintf("gs://%v/%v/%v", bucketName, datasetID, avroFile))
	gcsRef.SourceFormat = bigquery.Avro
	gcsRef.Schema = tableSchema
	loader := client.Dataset(datasetID).Table(tableID).LoaderFrom(gcsRef)
	loader.WriteDisposition = bigquery.WriteAppend
	job, err := loader.Run(ctx)
	if err != nil {
		return err
	}
	status, err := job.Wait(ctx)
	if err != nil {
		return err
	}
	if status.Err() != nil {
		return fmt.Errorf("job completed with error: %v", status.Err())
	}
	return nil
}
