package handlers

import (
	"log"
	"net/http"
)

func TestJSONPost(w http.ResponseWriter, r *http.Request) {
	jtaData := NewJTB()

	if err := jtaData.LoadFromJSON(r); err != nil {
		ErrorWithJSON(w, "JSON Data is invalid.", http.StatusBadRequest)
		return
	}
	if err := jtaData.Validate(); err != nil {
		ErrorWithJSON(w, err.Error(), http.StatusBadRequest)
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
