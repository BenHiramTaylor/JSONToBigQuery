package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"

	"github.com/gorilla/mux"
)

type jtaRequest struct {
	ProjectID   string                   `json:"ProjectID"`
	DatasetName string                   `json:"DatasetName"`
	TableName   string                   `json:"TableName"`
	Data        []map[string]interface{} `json:"Data"`
}

func helloWorld(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintln(w, "Hello World!")
}

func TestJSONPost(w http.ResponseWriter, r *http.Request) {
	var jtaData jtaRequest

	defer r.Body.Close()
	reqBody, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Failed to read passed bytes", http.StatusBadRequest)
		return
	}
	err = json.Unmarshal(reqBody, &jtaData)
	if err != nil {
		http.Error(w, "JSON Data is invalid.", http.StatusBadRequest)
		return
	}
	if len(jtaData.ProjectID) == 0 {
		http.Error(w, "JSON Data is invalid, ProjectID is missing.", http.StatusBadRequest)
		return
	} else if len(jtaData.DatasetName) == 0 {
		http.Error(w, "JSON Data is invalid, DatasetName is missing.", http.StatusBadRequest)
		return
	} else if len(jtaData.TableName) == 0 {
		http.Error(w, "JSON Data is invalid, TableName is missing.", http.StatusBadRequest)
		return
	} else if len(jtaData.Data) == 0 {
		http.Error(w, "JSON Data is invalid, Data array is empty.", http.StatusBadRequest)
		return
	}
	err = json.NewEncoder(w).Encode(jtaData)
	if err != nil {
		panic(err)
	}
}

func main() {
	port := ":8080"
	r := mux.NewRouter()
	r.HandleFunc("/HelloWorld", helloWorld)
	r.HandleFunc("/TestJSONPost", TestJSONPost).Methods("POST")
	fmt.Println("Listening on port", port)
	log.Fatal(http.ListenAndServe(port, r))
}
