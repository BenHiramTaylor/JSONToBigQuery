package handlers

import (
	"log"
	"net/http"

	"github.com/BenHiramTaylor/JSONToBigQuery/data"
)

func TestJSONPost(w http.ResponseWriter, r *http.Request) {
	jtaData := data.NewJTB()

	if err := jtaData.LoadFromJSON(r); err != nil {
		data.ErrorWithJSON(w, "JSON Data is invalid.", http.StatusBadRequest)
		return
	}
	if err := jtaData.Validate(); err != nil {
		data.ErrorWithJSON(w, err.Error(), http.StatusBadRequest)
		return
	}
	resp, err := jtaData.DumpToJSON()
	if err != nil {
		log.Fatalln("Can't Marshal JSON.")
	}
	w.Header().Add("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(resp)
}
