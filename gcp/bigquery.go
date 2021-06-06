package gcp

import (
	"context"

	"cloud.google.com/go/bigquery"
)

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
