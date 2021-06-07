package gcp

import (
	"context"
	"encoding/json"
	"log"

	"cloud.google.com/go/bigquery"
	"github.com/BenHiramTaylor/JSONToBigQuery/avro"
	"google.golang.org/api/option"
)

func GetBQClient(credsMap map[string]interface{}, projectID string) (*bigquery.Client, error) {
	credsJSON, err := json.Marshal(credsMap)
	if err != nil {
		return nil, err
	}
	ctx := context.Background()
	client, err := bigquery.NewClient(ctx, projectID, option.WithCredentialsJSON(credsJSON))
	if err != nil {
		return nil, err
	}
	log.Println("Created BigQuery Client")
	return client, nil
}

func updateTableAddColumn(client *bigquery.Client, datasetID, tableID, fieldName string, fieldType bigquery.FieldType) error {
	ctx := context.Background()
	tableRef := client.Dataset(datasetID).Table(tableID)
	meta, err := tableRef.Metadata(ctx)
	if err != nil {
		return err
	}
	newSchema := append(meta.Schema,
		&bigquery.FieldSchema{Name: fieldName, Type: fieldType},
	)
	update := bigquery.TableMetadataToUpdate{
		Schema: newSchema,
	}
	if _, err := tableRef.Update(ctx, update, meta.ETag); err != nil {
		return err
	}
	return nil
}

func getTableSchema(client *bigquery.Client, datasetID, tableID string) ([]*bigquery.FieldSchema, error) {
	ctx := context.Background()
	tableMeta, err := client.Dataset(datasetID).Table(tableID).Metadata(ctx)
	if err != nil {
		return nil, err
	}
	return tableMeta.Schema, nil
}

func CreateTable(client *bigquery.Client, datasetID, tableID string) error {
	ctx := context.Background()
	err := client.Dataset(datasetID).Create(ctx, &bigquery.DatasetMetadata{Name: datasetID})
	if err != nil {
		return err
	}
	err = client.Dataset(datasetID).Table(tableID).Create(ctx, &bigquery.TableMetadata{Name: tableID})
	if err != nil {
		return err
	}
	return nil
}

func EnsureSchema(client *bigquery.Client, datasetID, tableID string, sch avro.Schema) error {
	tableSchema, err := getTableSchema(client, datasetID, tableID)
	if err != nil {
		log.Printf("ERROR GETTING TABLE SCHEMA: %v", err.Error())
		return err
	}
	for _, af := range sch.Fields {
		for _, tf := range tableSchema {
			if af.Name == tf.Name {
				continue
			} else {
				// TODO MAP THE FIELD HERE USING CONST MAP
				err := updateTableAddColumn(client, datasetID, tableID, af.Name, bigquery.StringFieldType)
				if err != nil {
					return err
				}
			}
		}
	}
	return nil
}
