package data

import (
	"context"
	"encoding/json"
	"net/http"

	"cloud.google.com/go/bigquery"
	"github.com/go-playground/validator"
)

type JTBRequest struct {
	ProjectID       string                   `json:"ProjectID" validate:"required"`
	DatasetName     string                   `json:"DatasetName" validate:"required"`
	TableName       string                   `json:"TableName" validate:"required"`
	IdField         string                   `json:"IdField" validate:"required"`
	Query           string                   `json:"Query"`
	TimestampFormat string                   `json:"TimestampFormat"`
	Data            []map[string]interface{} `json:"Data" validate:"required"`
}

func (j *JTBRequest) ExecuteQuery() error {
	if j.Query != "" {
		ctx := context.Background()
		client, err := bigquery.NewClient(ctx, j.ProjectID)
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

func NewJTB() *JTBRequest {
	return new(JTBRequest)
}

// Validate Validates using the tags on the struct
func (j *JTBRequest) Validate() error {
	v := validator.New()
	return v.Struct(j)
}

func (j *JTBRequest) LoadFromJSON(r *http.Request) error {
	defer r.Body.Close()
	return json.NewDecoder(r.Body).Decode(&j)
}

func (j *JTBRequest) DumpToJSON() ([]byte, error) {
	return json.Marshal(j)
}
