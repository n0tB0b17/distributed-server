package server

import (
	"encoding/json"
	"fmt"
	"net/http"
)

type httpServer struct {
	log *Log
}

type producerRequest struct {
	R Record `json:"R"`
}

type producerResponse struct {
	Offset uint64 `json:"Offset"`
}

type consumerRequest struct {
	Offset uint64 `json:"Offset"`
}

type consumerResponse struct {
	R Record `json:"R"`
}

func HttpServer() *httpServer {
	return &httpServer{
		log: NewLog(),
	}
}

func (S *httpServer) handleProduce(w http.ResponseWriter, r *http.Request) {
	var req producerRequest
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		fmt.Println("[POST|PRODUCE] > Error while decoding request's body:", err.Error())
		http.Error(w, err.Error(), 404)
		return
	}

	off, err := S.log.append(req.R)
	if err != nil {
		fmt.Println("[POST|PRODUCE] > Error while appending log:", err.Error())
		http.Error(w, err.Error(), 404)
		return
	}

	res := producerResponse{Offset: off}
	err = json.NewEncoder(w).Encode(res)
	if err != nil {
		fmt.Println("[POST|PRODUCE] > Error while encoding response: ", err.Error())
		http.Error(w, err.Error(), 404)
		return
	}
}

func (S *httpServer) handleConsume(w http.ResponseWriter, r *http.Request) {
	var req consumerRequest
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		fmt.Println("[POST|CONSUMER] > Error while decoding request's body:", err.Error())
		http.Error(w, err.Error(), 404)
		return
	}

	rec, err := S.log.get(req.Offset)
	if err != nil {
		fmt.Println("[POST|CONSUMER] > Error while getting log: ", err.Error())
		http.Error(w, err.Error(), 404)
		return
	}

	res := consumerResponse{R: rec}
	err = json.NewEncoder(w).Encode(res)
	if err != nil {
		fmt.Println("[POST|CONSUME] > Error while encoding response: ", err.Error())
		http.Error(w, err.Error(), 404)
		return
	}
}
