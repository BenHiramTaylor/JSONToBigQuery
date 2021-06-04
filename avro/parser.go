package avro

import (
	"fmt"

	"github.com/BenHiramTaylor/JSONToBigQuery/dataTypes"
)

func ParseRequest(request *dataTypes.JtBRequest) error {
	avroName := fmt.Sprintf("%v.%v", request.DatasetName, request.TableName)
	avroNameSpace := fmt.Sprintf("%v.avsc", avroName)
	// TODO FINISH PARSING
}
func ParseRecord(rec map[string]interface{}) {

}
