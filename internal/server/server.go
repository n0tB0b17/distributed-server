package server

import (
	"context"
	"fmt"
	"net/http"

	"github.com/gorilla/mux"
	api "github.com/n0tB0b17/distri/api/v1"
)

type Config struct {
	log Log
}

type grpcServer struct {
	api.UnimplementedLogServer
	*Config
}

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

var _ api.LogServer = (*grpcServer)(nil)

func newGRPCServer(c *Config) (srv *grpcServer, err error) {
	return &grpcServer{
		Config: c,
	}, nil
}

func (S *grpcServer) Produce(ctx context.Context, in *api.ProduceRequest) (res *api.ProduceResponse, err error) {
	off, err := S.log.append(in.Record)
	if err != nil {
		fmt.Println(err.Error(), "Inside of SERVICE::Produce")
		return &api.ProduceResponse{}, err
	}
	return &api.ProduceResponse{Offset: off}, nil
}
