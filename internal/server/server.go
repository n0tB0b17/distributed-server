package server

import (
	"context"
	"fmt"
	"net/http"

	"github.com/gorilla/mux"
	log_v1 "github.com/n0tB0b17/distri/api/v1"
)

type Config struct {
	log CommitLog
}

type grpcServer struct {
	log_v1.UnimplementedLogServer
	*Config
}

type CommitLog interface {
	Append(*log_v1.Record) (uint64, error)
	Read(uint64) (*log_v1.Record, error)
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

var _ log_v1.LogServer = (*grpcServer)(nil)

func newGRPCServer(c *Config) (srv *grpcServer, err error) {
	return &grpcServer{
		Config: c,
	}, nil
}

func (S *grpcServer) Produce(ctx context.Context, in *log_v1.ProduceRequest) (res *log_v1.ProduceResponse, err error) {
	off, err := S.log.Append(in.Record)
	if err != nil {
		fmt.Println("[GRPCSERVER | PRODUCE] > Error while running Produce function of GRPC service", err.Error())
		return nil, err
	}
	return &log_v1.ProduceResponse{Offset: off}, nil
}

func (S *grpcServer) Consume(ctx context.Context, in *log_v1.ConsumeRequest) (*log_v1.ConsumeResponse, error) {
	record, err := S.log.Read(in.Offset)
	if err != nil {
		fmt.Println("[GRPCSERVER | CONSUME] > Error while running Consume function of GRPC service", err.Error())
		return nil, err
	}

	return &log_v1.ConsumeResponse{Record: record}, nil
}

func (S *grpcServer) ProduceStream(stream log_v1.Log_ProduceStreamServer) error {
	for {
		req, err := stream.Recv()
		if err != nil {
			fmt.Println("[GRPCSERVER | PRODUCE | STREAM] > Error while running produce stream function of GRPC service", err.Error())
			return err
		}

		res, err := S.Produce(stream.Context(), req)
		if err != nil {
			fmt.Println("[GRPCSERVER | PRODUCE | STREAM] > Error while running produce function inside of Produce-Stream", err.Error())
			return err
		}

		if err = stream.Send(res); err != nil {
			fmt.Println("[GRPCSERVER | PRODUCE | STREAM] > Error while running stream.Send function", err.Error())
			return err
		}
	}
}

func (S *grpcServer) ConsumeStream(req *log_v1.ConsumeRequest, stream log_v1.Log_ConsumeStreamServer) error {
	for {
		select {
		case <-stream.Context().Done():
			return nil
		default:
			res, err := S.Consume(stream.Context(), req)
			switch err.(type) {
			case nil:
			// case log_v1.ErrOffsetOutOfRange:
			// 	continue
			default:
				return err
			}
			if err = stream.Send(res); err != nil {
				return err
			}
			req.Offset++
		}
	}
}
