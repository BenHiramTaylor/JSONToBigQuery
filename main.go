package main

import (
	"fmt"
	"log"
	"net/http"

	"github.com/BenHiramTaylor/JSONToBigQuery/handlers"
	"github.com/gorilla/mux"
)

func main() {
	port := ":8080"
	r := mux.NewRouter()
	r.HandleFunc("/TestJSONPost", handlers.TestJSONPost).Methods("POST")
	fmt.Println("Listening on port", port)
	log.Fatal(http.ListenAndServe(port, r))
}
