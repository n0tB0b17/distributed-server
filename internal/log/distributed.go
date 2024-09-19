package log

import (
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/hashicorp/raft"
	log_v1 "github.com/n0tB0b17/distri/api/v1"
	"google.golang.org/protobuf/proto"
)

type DistributedLog struct {
	config Config
	log    *Log
	raft   raft.Raft
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
	fms := &fms{
		log: DL.log,
	}
	fmt.Println(fms)
	return nil
}

type fms struct {
	log *Log
}

var _ raft.FSM = (*fms)(nil)

// function implementation for raft.FMS
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

func (f *fms) Snapshot() (raft.FSMSnapshot, error) {
	r := f.log.Reader()
	return &snapshot{reader: r}, nil
}

func (f *fms) Restore(r io.ReadCloser) error {
	return nil
}
