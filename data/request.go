package data

import (
	"context"
	"encoding/json"
	"net/http"

	"cloud.google.com/go/bigquery"
	"github.com/go-playground/validator"
)

// JTBRequest Request object, represents the request that you parse to this
// service, validate:"required" tags mean the value has to be present in the
// body.
type JTBRequest struct {
	ProjectID       string                   `json:"ProjectID" validate:"required"`
	DatasetName     string                   `json:"DatasetName" validate:"required"`
	TableName       string                   `json:"TableName" validate:"required"`
	IdField         string                   `json:"IdField" validate:"required"`
	Query           string                   `json:"Query"`
	TimestampFormat string                   `json:"TimestampFormat"`
	Data            []map[string]interface{} `json:"Data" validate:"required"`
}

// ExecuteQuery Executes the query in the request assuming it is not blank
func (j *JTBRequest) ExecuteQuery() error {
	if j.Query != "" {
		ctx := context.Background()
		defer ctx.Done()
		client, err := bigquery.NewClient(ctx, j.ProjectID)
		defer client.Close()
		if err != nil {
			return err
		}
		q := client.Query(j.Query)
		q.Location = "US"
		job, err := q.Run(ctx)
		if err != nil {
			return err
		}
		_, err = job.Wait(ctx)
		if err != nil {
			return err
		}
		return nil
	} else {
		return nil
	}
}

// NewJTB Constructor function, returns blank JTBRequest to have json loaded into it
func NewJTB() *JTBRequest {
	return new(JTBRequest)
}

// Validate Validates using the tags on the struct
func (j *JTBRequest) Validate() error {
	v := validator.New()
	return v.Struct(j)
}

// LoadFromJSON Loads the struct values from a http.request body
func (j *JTBRequest) LoadFromJSON(r *http.Request) error {
	defer r.Body.Close()
	return json.NewDecoder(r.Body).Decode(&j)
}
