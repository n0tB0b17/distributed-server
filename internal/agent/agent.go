package agent

import (
	"crypto/tls"
	"fmt"
	"net"
	"sync"

	log_v1 "github.com/n0tB0b17/distri/api/v1"
	"github.com/n0tB0b17/distri/internal/auth"
	"github.com/n0tB0b17/distri/internal/discovery"
	log "github.com/n0tB0b17/distri/internal/log"
	"github.com/n0tB0b17/distri/internal/server"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

type Config struct {
	ServerTLSConfig *tls.Config
	PeerTLSConfig   *tls.Config
	DataDir         string
	BindAddr        string
	RPCPort         int
	NodeName        string
	StartJoinsAddr  []string
	ACLModelFile    string
	ACLPolicyFile   string
}

type Agent struct {
	Config
	log        *log.Log
	server     *grpc.Server
	membership *discovery.Membership
	replicator *log.Replicator

	shutdown     bool
	shutdowns    chan struct{}
	shutdownLock sync.Mutex
}

func (C *Config) RPCAddr() (string, error) {
	host, _, err := net.SplitHostPort(C.BindAddr)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s:%d", host, C.RPCPort), nil
}

func NewAgent(C Config) (*Agent, error) {
	a := &Agent{
		Config:    C,
		shutdowns: make(chan struct{}),
	}

	setup := []func() error{
		a.setupLog,
		a.setupLogger,
		a.setupServer,
		a.setupMembership,
	}

	for _, fn := range setup {
		if err := fn(); err != nil {
			return nil, err
		}
	}
	return a, nil
}

func (A *Agent) setupLogger() error {
	logger, err := zap.NewDevelopment()
	if err != nil {
		return err
	}
	zap.ReplaceGlobals(logger)
	return nil
}

func (A *Agent) setupLog() error {
	var err error
	A.log, err = log.NewLog(
		A.Config.DataDir,
		log.Config{},
	)

	return err
}

func (A *Agent) setupServer() error {
	authorizer := auth.New(
		A.Config.ACLModelFile,
		A.Config.ACLPolicyFile,
	)

	serverConfig := &server.Config{
		CommitLog:  A.log,
		Authorizer: authorizer,
	}

	var opts []grpc.ServerOption
	if A.Config.ServerTLSConfig != nil {
		tlsCred := credentials.NewTLS(A.Config.ServerTLSConfig)
		opts = append(opts, grpc.Creds(tlsCred))
	}

	var err error
	A.server, err = server.NewGRPCServer(serverConfig, opts...)
	if err != nil {
		return err
	}

	var addr string
	addr, err = A.RPCAddr()
	if err != nil {
		return err
	}

	var listener net.Listener
	listener, err = net.Listen("tcp", addr)
	if err != nil {
		return err
	}

	go func(A *Agent) {
		if err = A.server.Serve(listener); err != nil {
			_ = A.ShutDown()
		}
	}(A)
	return err
}

func (A *Agent) setupMembership() error {
	rpc, err := A.Config.RPCAddr()
	if err != nil {
		return err
	}

	var opts []grpc.DialOption
	if A.PeerTLSConfig != nil {
		opts = append(opts, grpc.WithTransportCredentials(
			credentials.NewTLS(A.PeerTLSConfig),
		))
	}

	conn, err := grpc.Dial(rpc, opts...)
	if err != nil {
		return err
	}

	client := log_v1.NewLogClient(conn)
	A.replicator = &log.Replicator{
		DialOptions: opts,
		LocalServer: client,
	}

	discfg := discovery.Config{
		NodeName: A.NodeName,
		BindAddr: A.BindAddr,
		Tags: map[string]string{
			"rpc_addr": rpc,
		},
		StartJoinAddr: A.StartJoinsAddr,
	}
	A.membership, err = discovery.New(
		nil,
		discfg,
	)

	if err != nil {
		return err
	}

	return nil
}

func (A *Agent) ShutDown() error {
	A.shutdownLock.Lock()
	defer A.shutdownLock.Unlock()

	if A.shutdown {
		return nil
	}

	A.shutdown = true
	close(A.shutdowns)

	shutdown := []func() error{
		A.membership.Leave,
		A.replicator.Close,
		func() error {
			A.server.GracefulStop()
			return nil
		},
		A.log.Close,
	}

	for _, shutdownFN := range shutdown {
		if err := shutdownFN(); err != nil {
			return err
		}
	}

	return nil
}
