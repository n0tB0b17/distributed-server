package server

import (
	"context"
	"fmt"
	"strings"
	"time"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_auth "github.com/grpc-ecosystem/go-grpc-middleware/auth"
	grpc_zap "github.com/grpc-ecosystem/go-grpc-middleware/logging/zap"
	grpc_ctxtags "github.com/grpc-ecosystem/go-grpc-middleware/tags"
	log_v1 "github.com/n0tB0b17/distri/api/v1"
	"go.opencensus.io/plugin/ocgrpc"
	"go.opencensus.io/stats/view"
	"go.opencensus.io/trace"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

const (
	objectWildCard = "*"
	produceAction  = "produce"
	consumeAction  = "consume"
)

type Config struct {
	log        CommitLog
	authorizer Authorizer
}

type Authorizer interface {
	Authorizer(subject, object, action string) error
}

type grpcServer struct {
	log_v1.UnimplementedLogServer
	*Config
}

type CommitLog interface {
	Append(*log_v1.Record) (uint64, error)
	Read(uint64) (*log_v1.Record, error)
}

var _ log_v1.LogServer = (*grpcServer)(nil)

func newGRPCServer(c *Config) (srv *grpcServer, err error) {
	return &grpcServer{
		Config: c,
	}, nil
}

func NewGRPCServer(c *Config, opts ...grpc.ServerOption) (*grpc.Server, error) {
	l := zap.L().Named("server")
	zapOptions := []grpc_zap.Option{
		grpc_zap.WithDurationField(
			func(duration time.Duration) zapcore.Field {
				return zap.Int64(
					"grpc.time_ns",
					duration.Nanoseconds(),
				)
			},
		),
	}

	sampler := trace.ProbabilitySampler(0.5)
	trace.ApplyConfig(
		trace.Config{
			DefaultSampler: func(p trace.SamplingParameters) trace.SamplingDecision {
				if strings.Contains(p.Name, "produce") {
					return trace.SamplingDecision{Sample: true}
				}
				return sampler(p)
			},
		},
	)

	err := view.Register(ocgrpc.DefaultServerViews...)
	if err != nil {
		return nil, err
	}

	opts = append(opts, grpc.StreamInterceptor(
		grpc_middleware.ChainStreamServer(
			grpc_ctxtags.StreamServerInterceptor(),
			grpc_zap.StreamServerInterceptor(l, zapOptions...),
			grpc_auth.StreamServerInterceptor(authenticate),
		)), grpc.UnaryInterceptor(
		grpc_middleware.ChainUnaryServer(
			grpc_ctxtags.UnaryServerInterceptor(),
			grpc_zap.UnaryServerInterceptor(l, zapOptions...),
			grpc_auth.UnaryServerInterceptor(authenticate),
		),
	), grpc.StatsHandler(&ocgrpc.ServerHandler{}))

	grpcServer := grpc.NewServer(opts...)
	srv, err := newGRPCServer(c)
	if err != nil {
		return nil, err
	}

	log_v1.RegisterLogServer(grpcServer, srv)
	return grpcServer, nil
}

func (S *grpcServer) Produce(ctx context.Context, in *log_v1.ProduceRequest) (res *log_v1.ProduceResponse, err error) {
	if err := S.authorizer.Authorizer(
		subject(ctx),
		objectWildCard,
		produceAction,
	); err != nil {
		return nil, err
	}

	off, err := S.log.Append(in.Record)
	if err != nil {
		fmt.Println("[GRPCSERVER | PRODUCE] > Error while running Produce function of GRPC service", err.Error())
		return nil, err
	}
	return &log_v1.ProduceResponse{Offset: off}, nil
}

func (S *grpcServer) Consume(ctx context.Context, in *log_v1.ConsumeRequest) (*log_v1.ConsumeResponse, error) {
	if err := S.authorizer.Authorizer(
		subject(ctx),
		objectWildCard,
		consumeAction,
	); err != nil {
		return nil, err
	}
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
			case log_v1.ErrOffsetOutOfRange:
				continue
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

func subject(ctx context.Context) string {
	return ctx.Value(subjectContextKey{}).(string)
}

type subjectContextKey struct{}

func authenticate(ctx context.Context) (context.Context, error) {
	p, ok := peer.FromContext(ctx)
	if !ok {
		return ctx, status.New(
			codes.Unknown,
			"couldnt find peer information",
		).Err()
	}

	if p.AuthInfo == nil {
		return context.WithValue(ctx, subjectContextKey{}, ""), nil
	}

	tlsInfo := p.AuthInfo.(credentials.TLSInfo)
	subject := tlsInfo.State.VerifiedChains[0][0].Subject.CommonName
	ctx = context.WithValue(ctx, subjectContextKey{}, subject)

	return ctx, nil
}
