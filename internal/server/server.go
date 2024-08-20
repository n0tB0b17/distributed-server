package server

import (
	"net/http"

	"github.com/gorilla/mux"
)

func NewHttpServer(addr string) *http.Server {
	srv := HttpServer()
	router := mux.NewRouter()

	router.HandleFunc("/", srv.handleProduce).Methods("POST")
	router.HandleFunc("/", srv.handleConsume).Methods("GET")
	return &http.Server{
		Addr:    addr,
		Handler: router,
	}
}
