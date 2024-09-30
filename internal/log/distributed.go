package log

import (
	"bytes"
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
	log_v1 "github.com/n0tB0b17/distri/api/v1"
	"google.golang.org/protobuf/proto"
)

type DistributedLog struct {
	config Config
	log    *Log
	raft   *raft.Raft
}

func NewDistributedLog(datadir string, cfg Config) (*DistributedLog, error) {
	dl := &DistributedLog{
		config: cfg,
	}

	if err := dl.setupLog(datadir); err != nil {
		return nil, err
	}

	if err := dl.setupRaft(datadir); err != nil {
		return nil, err
	}

	return dl, nil
}

func (DL *DistributedLog) setupLog(datadir string) error {
	logPath := filepath.Join(datadir, "log")
	if err := os.MkdirAll(logPath, 0755); err != nil {
		return err
	}

	var err error
	DL.log, err = NewLog(logPath, DL.config)
	return err
}

func (DL *DistributedLog) setupRaft(datadir string) error {
	fms := &fms{log: DL.log}
	logPath := filepath.Join(datadir, "raft", "log")
	if err := os.MkdirAll(logPath, 0755); err != nil {
		return err
	}

	logConfig := DL.config
	logConfig.Segment.InitialOffset = 1
	logStore, err := newLogStore(logPath, logConfig)
	if err != nil {
		return err
	}

	stableStore, err := raftboltdb.NewBoltStore(filepath.Join(datadir, "raft", "stable"))
	if err != nil {
		return err
	}

	snapshotStore, err := raft.NewFileSnapshotStore(
		filepath.Join(datadir, "raft"),
		1,
		os.Stderr,
	)
	if err != nil {
		return err
	}

	transport := raft.NewNetworkTransport(
		nil,
		5,
		10*time.Second,
		os.Stderr,
	)

	cfg := raft.DefaultConfig()
	cfg.LocalID = DL.config.Raft.LocalID
	if DL.config.Raft.HeartbeatTimeout != 0 {
		cfg.HeartbeatTimeout = DL.config.Raft.HeartbeatTimeout
	}

	if DL.config.Raft.ElectionTimeout != 0 {
		cfg.ElectionTimeout = DL.config.Raft.ElectionTimeout
	}

	if DL.config.Raft.LeaderLeaseTimeout != 0 {
		cfg.LeaderLeaseTimeout = DL.config.Raft.LeaderLeaseTimeout
	}

	if DL.config.Raft.CommitTimeout != 0 {
		cfg.CommitTimeout = DL.config.Raft.CommitTimeout
	}

	DL.raft, err = raft.NewRaft(
		cfg,
		fms,
		logStore,
		stableStore,
		snapshotStore,
		transport,
	)

	if err != nil {
		return err
	}

	hasExisted, err := raft.HasExistingState(
		logStore,
		stableStore,
		snapshotStore,
	)

	if err != nil {
		return err
	}

	if DL.config.Raft.Bootstrap && !hasExisted {
		raftServer := []raft.Server{{
			ID:      cfg.LocalID,
			Address: transport.LocalAddr(),
		}}
		raftConfig := raft.Configuration{
			Servers: raftServer,
		}

		err = DL.raft.BootstrapCluster(raftConfig).Error()
	}
	return err
}

func (DL *DistributedLog) Append(record *log_v1.Record) (uint64, error) {
	resp, err := DL.apply(AppendRequestType, &log_v1.ProduceRequest{Record: record})
	if err != nil {
		return 0, err
	}

	return resp.(*log_v1.ProduceResponse).Offset, nil
}

func (DL *DistributedLog) apply(reqType RequestType, protoMsg proto.Message) (interface{}, error) {
	var buf bytes.Buffer
	_, err := buf.Write([]byte{byte(reqType)})
	if err != nil {
		return nil, err
	}

	b, err := proto.Marshal(protoMsg)
	if err != nil {
		return nil, err
	}

	_, err = buf.Write(b)
	if err != nil {
		return nil, err
	}

	timeout := 10 * time.Second
	applyFuture := DL.raft.Apply(buf.Bytes(), timeout)
	if applyFuture.Error() != nil {
		return nil, applyFuture.Error()
	}

	resp := applyFuture.Response()
	if err := resp.(error); err != nil {
		return nil, err
	}

	return resp, nil
}

func (DL *DistributedLog) Read(offset uint64) (*log_v1.Record, error) { return DL.log.Read(offset) }
func (DL *DistributedLog) Join(id, addr string) error {
	raftCfg := DL.raft.GetConfiguration()
	if err := raftCfg.Error(); err != nil {
		return err
	}

	raftID := raft.ServerID(id)
	raftServer := raft.ServerAddress(addr)

	for _, srv := range raftCfg.Configuration().Servers {
		if srv.ID == raftID || srv.Address == raftServer {
			if srv.ID == raftID && srv.Address == raftServer {
				// server already joined
				return nil
			}
			// remove existing server
			raftDel := DL.raft.RemoveServer(raftID, 0, 0)
			if err := raftDel.Error(); err != nil {
				return err
			}
		}
	}

	raftAdd := DL.raft.AddVoter(raftID, raftServer, 0, 0)
	if err := raftAdd.Error(); err != nil {
		return err
	}

	return nil
}

func (DL *DistributedLog) Leave(id string) error {
	removeFuture := DL.raft.RemoveServer(raft.ServerID(id), 0, 0)
	return removeFuture.Error()
}

func (DL *DistributedLog) WaitForLeader(dur time.Duration) error {
	timeoutAfter := time.After(dur)
	tTicker := time.NewTicker(time.Second)
	defer tTicker.Stop()
	for {
		select {
		case <-timeoutAfter:
			return fmt.Errorf("timeout error")
		case <-tTicker.C:
			if l := DL.raft.Leader(); l != "" {
				return nil
			}
		}
	}
}

func (DL *DistributedLog) Close() error {
	f := DL.raft.Shutdown()
	if err := f.Error(); err != nil {
		return err
	}

	return DL.log.Close()
}

// function implementation for raft.FSM
// fsm stands for > finite-state-machine
type fms struct {
	log *Log
}

// should implement following functions
// 1.Apply | 2.Snapshot | 3.Restore
var _ raft.FSM = (*fms)(nil)

type RequestType uint8

const AppendRequestType RequestType = 0

func (f *fms) Apply(record *raft.Log) interface{} {
	buf := record.Data
	reqType := RequestType(buf[0])
	switch reqType {
	case AppendRequestType:
		f.applyAppend(buf[1:])
	}

	return nil
}

func (l *fms) applyAppend(b []byte) interface{} {
	var req log_v1.ProduceRequest
	err := proto.Unmarshal(b, &req)
	if err != nil {
		return nil
	}

	offset, err := l.log.Append(req.Record)
	if err != nil {
		return err
	}

	return &log_v1.ProduceResponse{Offset: offset}
}

func (f *fms) Snapshot() (raft.FSMSnapshot, error) {
	r := f.log.Reader()
	return &snapshot{reader: r}, nil
}

func (f *fms) Restore(r io.ReadCloser) error {
	b := make([]byte, lenWidth)
	var buf bytes.Buffer
	for i := 0; ; i++ {
		_, err := io.ReadFull(r, b)
		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}

		size := int64(binary.LittleEndian.Uint64(b))
		if _, err := io.CopyN(&buf, r, size); err != nil {
			return err
		}

		record := &log_v1.Record{}
		if err := proto.Unmarshal(buf.Bytes(), record); err != nil {
			return err
		}

		if i == 0 {
			f.log.config.Segment.InitialOffset = record.Offset
			if err := f.log.Reset(); err != nil {
				return err
			}
		}

		if _, err := f.log.Append(record); err != nil {
			return err
		}

		buf.Reset()
	}
	return nil
}

// end of FMS

// log repliacation and persistence
var _ raft.FSMSnapshot = (*snapshot)(nil)

type snapshot struct {
	reader io.Reader
}

func (s *snapshot) Release() {}
func (s *snapshot) Persist(sink raft.SnapshotSink) error {
	if _, err := io.Copy(sink, s.reader); err != nil {
		_ = sink.Cancel()
		return err
	}

	return sink.Close()
}

type logStore struct {
	*Log
}

// storing our custom logs to raft store
var _ raft.LogStore = (*logStore)(nil)

func newLogStore(dir string, cfg Config) (*logStore, error) {
	log, err := NewLog(dir, cfg)
	if err != nil {
		return nil, err
	}

	return &logStore{log}, nil
}

func (L *logStore) FirstIndex() (uint64, error) {
	return L.LowestOffset()
}
func (L *logStore) LastIndex() (uint64, error) {
	return L.HighestOffset()
}
func (L *logStore) GetLog(index uint64, out *raft.Log) error {
	record, err := L.Read(index)
	if err != nil {
		return err
	}

	out.Index = record.Offset
	out.Data = record.Value
	out.Type = raft.LogType(record.Type)
	out.Term = record.Term
	return nil
}

func (L *logStore) StoreLog(record *raft.Log) error {
	return L.StoreLogs([]*raft.Log{record})
}

func (L *logStore) StoreLogs(records []*raft.Log) error {
	for _, record := range records {
		if _, err := L.Append(&log_v1.Record{
			Value: record.Data,
			Term:  record.Term,
			Type:  uint32(record.Type),
		}); err != nil {
			return err
		}
	}
	return nil
}
func (L *logStore) DeleteRange(max, min uint64) error {
	return L.Truncate(max)
}

// type StreamLayer interface{
// 	net.Listener
// 	Dial(addr raft.ServerAddress, timeout time.Duration) (net.Conn, error)
// }

// stream layer
// tls for encryped connection
type StreamLayer struct {
	listener        net.Listener
	serverTLSConfig *tls.Config
	peerTLSConfig   *tls.Config
}

var _ raft.StreamLayer = (*StreamLayer)(nil)

// for multiplex raft on same port
const RaftRPC = 1

func NewStreamLayer(
	listener net.Listener,
	serverTLSConfig,
	peerTLSConfig *tls.Config,
) *StreamLayer {
	return &StreamLayer{
		listener:        listener,
		serverTLSConfig: serverTLSConfig,
		peerTLSConfig:   peerTLSConfig,
	}
}

// makes a outgoing connection to server in a raft cluster.
func (S *StreamLayer) Dial(
	addr raft.ServerAddress,
	timeout time.Duration,
) (net.Conn, error) {
	dialer := &net.Dialer{Timeout: timeout}
	conn, err := dialer.Dial("tcp", string(addr))
	if err != nil {
		return nil, err
	}

	_, err = conn.Write([]byte{byte(RaftRPC)})
	if err != nil {
		return nil, err
	}

	if S.peerTLSConfig != nil {
		conn = tls.Client(conn, S.peerTLSConfig)
	}

	return conn, nil
}

func (S *StreamLayer) Close() error   { return S.listener.Close() }
func (S *StreamLayer) Addr() net.Addr { return S.listener.Addr() }
func (S *StreamLayer) Accept() (net.Conn, error) {
	conn, err := S.listener.Accept()
	if err != nil {
		return nil, err
	}

	bt := make([]byte, 1)
	_, err = conn.Write(bt)
	if err != nil {
		return nil, err
	}

	if comp := bytes.Compare([]byte{byte(RaftRPC)}, bt); comp != 0 {
		return nil, fmt.Errorf("Not a Raft RPC")
	}

	if S.serverTLSConfig != nil {
		return tls.Server(conn, S.serverTLSConfig), nil
	}

	return conn, nil
}
