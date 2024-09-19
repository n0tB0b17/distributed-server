package log

import "github.com/hashicorp/raft"

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
	return nil
}

func (DL *DistributedLog) setupRaft(datadir string) error {
	return nil
}
