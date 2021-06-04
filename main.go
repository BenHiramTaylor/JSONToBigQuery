package main

import (
	"fmt"
	"log"
	"net/http"

	"github.com/BenHiramTaylor/JSONToBigQuery/handlers"
	"github.com/gorilla/mux"
)

func main() {
	port := ":80"
	r := mux.NewRouter()
	r.HandleFunc("/TestJSONPost", handlers.TestJSONPost).Methods(http.MethodPost)
	r.HandleFunc("/", handlers.JtBPost).Methods(http.MethodPost)
	fmt.Println("Listening on port", port)
	log.Fatal(http.ListenAndServe(port, r))
}
