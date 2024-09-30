package agent

import (
	"bytes"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	"github.com/n0tB0b17/distri/internal/auth"
	"github.com/n0tB0b17/distri/internal/discovery"
	log "github.com/n0tB0b17/distri/internal/log"
	"github.com/n0tB0b17/distri/internal/server"
	"github.com/soheilhy/cmux"
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
	Bootstrap       bool
}

type Agent struct {
	Config
	log        *log.DistributedLog
	server     *grpc.Server
	membership *discovery.Membership
	replicator *log.Replicator
	mux        cmux.CMux

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
		a.setupMux,
		a.setupServer,
		a.setupMembership,
	}

	for _, fn := range setup {
		if err := fn(); err != nil {
			return nil, err
		}
	}

	go a.serve()
	return a, nil
}

func (A *Agent) setupMux() error {
	rpcAddr := fmt.Sprintf(":%d", A.Config.RPCPort)
	listener, err := net.Listen("tcp", rpcAddr)
	if err != nil {
		return err
	}

	A.mux = cmux.New(listener)
	return nil
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
	raftListener := A.mux.Match(func(rdr io.Reader) bool {
		b := make([]byte, 1)
		if _, err := rdr.Read(b); err != nil {
			return false
		}

		return bytes.Compare(b, []byte{byte(log.RaftRPC)}) == 0
	})

	logConfig := log.Config{}
	logConfig.Raft.StreamLayer = log.NewStreamLayer(
		raftListener,
		A.ServerTLSConfig,
		A.PeerTLSConfig,
	)
	logConfig.Raft.LocalID = raft.ServerID(A.NodeName)
	logConfig.Raft.Bootstrap = A.Bootstrap

	var err error
	A.log, err = log.NewDistributedLog(
		A.DataDir,
		logConfig,
	)
	if err != nil {
		return err
	}

	if A.Bootstrap {
		err = A.log.WaitForLeader(3 * time.Second)
	}

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

	grpcListener := A.mux.Match(cmux.Any())
	go func(lis net.Listener) {
		if err := A.server.Serve(lis); err != nil {
			_ = A.ShutDown()
		}
	}(grpcListener)

	return err
}

func (A *Agent) setupMembership() error {
	rpc, err := A.Config.RPCAddr()
	if err != nil {
		return err
	}

	disCfg := discovery.Config{
		NodeName:      A.NodeName,
		BindAddr:      A.BindAddr,
		StartJoinAddr: A.StartJoinsAddr,
		Tags: map[string]string{
			"rpc_addr": rpc,
		},
	}

	A.membership, err = discovery.New(A.log, disCfg)
	if err != nil {
		return err
	}
	return nil
}

func (A *Agent) serve() error {
	if err := A.mux.Serve(); err != nil {
		_ = A.ShutDown()
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
