package data

import (
	"encoding/json"
	"net/http"

	"github.com/go-playground/validator"
)

type JtBRequest struct {
	ProjectID       string                   `json:"ProjectID" validate:"required"`
	DatasetName     string                   `json:"DatasetName" validate:"required"`
	TableName       string                   `json:"TableName" validate:"required"`
	IdField         string                   `json:"IdField" validate:"required"`
	Query           string                   `json:"Query"`
	TimestampFormat string                   `json:"TimestampFormat"`
	Data            []map[string]interface{} `json:"Data" validate:"required"`
}

func NewJTB() *JtBRequest {
	return new(JtBRequest)
}

func (j *JtBRequest) Validate() error {
	v := validator.New()
	return v.Struct(j)
}

func (j *JtBRequest) LoadFromJSON(r *http.Request) error {
	defer r.Body.Close()
	return json.NewDecoder(r.Body).Decode(&j)
}

func (j *JtBRequest) DumpToJSON() ([]byte, error) {
	return json.Marshal(j)
}
