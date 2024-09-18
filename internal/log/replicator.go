package log

import (
	"context"
	"sync"

	log_v1 "github.com/n0tB0b17/distri/api/v1"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

type Replicator struct {
	DialOptions []grpc.DialOption
	LocalServer log_v1.LogClient
	logger      *zap.Logger
	mu          sync.Mutex
	servers     map[string]chan struct{}
	closed      bool
	close       chan struct{}
}

func (R *Replicator) replicate(addr string, leave chan struct{}) {
	cc, err := grpc.Dial(addr, R.DialOptions...)
	if err != nil {
		R.logError(
			err,
			err.Error(),
			addr,
		)
		return
	}
	defer cc.Close()
	ctx := context.Background()
	client := log_v1.NewLogClient(cc)

	stream, err := client.ConsumeStream(ctx, &log_v1.ConsumeRequest{Offset: 0})
	if err != nil {
		R.logError(err, err.Error(), addr)
		return
	}

	records := make(chan *log_v1.Record)
	go func() {
		for {
			res, err := stream.Recv()
			if err != nil {
				return
			}

			records <- res.Record
		}
	}()

	for {
		select {
		case <-R.close:
			return
		case <-leave:
			return

		case record := <-records:
			_, err := R.LocalServer.Produce(ctx, &log_v1.ProduceRequest{
				Record: record,
			})

			if err != nil {
				R.logError(err, err.Error(), addr)
				return
			}
		}
	}
}

func (R *Replicator) Leave(name string) error {
	R.mu.Lock()
	defer R.mu.Unlock()
	R.init()

	if _, ok := R.servers[name]; !ok {
		return nil
	}

	close(R.servers[name])
	delete(R.servers, name)
	return nil
}

func (R *Replicator) Close() error {
	R.mu.Lock()
	defer R.mu.Unlock()
	R.init()

	if R.closed {
		return nil
	}

	R.closed = true
	close(R.close)
	return nil
}

func (R *Replicator) init() {
	if R.logger == nil {
		R.logger = zap.L().Named("replicator")
	}

	if R.servers == nil {
		R.servers = make(map[string]chan struct{})
	}

	if R.close == nil {
		R.close = make(chan struct{})
	}
}

func (R *Replicator) logError(err error, msg, addr string) {
	R.logger.Error(
		msg,
		zap.String("addr", addr),
		zap.Error(err),
	)
}
