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
	bqSchemaMap = map[string]bigquery.FieldType{
		"string":                bigquery.StringFieldType,
		"int":                   bigquery.IntegerFieldType,
		"long":                  bigquery.IntegerFieldType,
		"float":                 bigquery.FloatFieldType,
		"double":                bigquery.FloatFieldType,
		"boolean":               bigquery.BooleanFieldType,
		"long.timestamp-micros": bigquery.TimestampFieldType,
	}
)

func GetBQClient(credsPath string, projectID string) (*bigquery.Client, error) {
	ctx := context.Background()
	client, err := bigquery.NewClient(ctx, projectID, option.WithCredentialsFile(credsPath))
	if err != nil {
		return nil, err
	}
	log.Println("Created BigQuery Client")
	return client, nil
}

func updateTableSchema(client *bigquery.Client, datasetID, tableID string, sch avro.Schema) error {
	var newSchema = bigquery.Schema{}
	ctx := context.Background()
	tableRef := client.Dataset(datasetID).Table(tableID)
	meta, err := tableRef.Metadata(ctx)
	if err != nil {
		return err
	}
	newSchema = append(meta.Schema)
	for _, af := range sch.Fields {
		exists := false
		afType := ""
		for _, tf := range newSchema {
			if af.Name == tf.Name {
				exists = true
				break
			} else {
				continue
			}
		}
		if !exists {
			for _, v := range af.FieldType {
				if v == "null" {
					continue
				} else {
					afType = v
				}
			}
			newSchema = append(newSchema,
				&bigquery.FieldSchema{Name: af.Name, Type: bqSchemaMap[afType]},
			)
		}
	}

	update := bigquery.TableMetadataToUpdate{
		Schema: newSchema,
	}
	if _, err := tableRef.Update(ctx, update, meta.ETag); err != nil {
		return err
	}
	return nil
}

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

func getTableSchema(client *bigquery.Client, datasetID, tableID string) (bigquery.Schema, error) {
	ctx := context.Background()
	tableRef := client.Dataset(datasetID).Table(tableID)
	meta, err := tableRef.Metadata(ctx)
	if err != nil {
		return nil, err
	}
	return meta.Schema, nil
}

func PrepareTable(client *bigquery.Client, datasetID, tableID string, sch avro.Schema) error {
	err := createTable(client, datasetID, tableID)
	if err != nil {
		return err
	}
	err = updateTableSchema(client, datasetID, tableID, sch)
	if err != nil {
		return err
	}
	return nil
}

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
