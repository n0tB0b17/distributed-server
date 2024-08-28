package log

import (
	"fmt"
	"os"
)

type segment struct {
	store            *Store
	idx              *Index
	config           Config
	baseOff, nextOff uint64
}

func NewSegment(dir string, baseOff uint64, c Config) (*segment, error) {
	s := &segment{
		baseOff: baseOff,
		config:  c,
	}

	storeFile, err := os.OpenFile(
		dir,
		os.O_RDWR|os.O_CREATE|os.O_APPEND,
		0644,
	)
	if err != nil {
		fmt.Println("[SEGMENT | STORE] > Error while opening store file", err.Error())
		return nil, err
	}

	store, err := NewStore(storeFile)
	if err != nil {
		fmt.Println("[SEGMENT | STORE] > Error while creating new store for given file", err.Error())
		return nil, err
	}
	s.store = store

	idxFile, err := os.OpenFile(
		"",
		os.O_RDWR|os.O_CREATE,
		0644,
	)
	if err != nil {
		fmt.Println("[SEGMENT | INDEX] > Error while opening index file", err.Error())
		return nil, err
	}

	idx, err := NewIndex(idxFile, c)
	if err != nil {
		fmt.Println("[SEGMENT | INDEX] > Error while creating new index for given file", err.Error())
		return nil, err
	}
	s.idx = idx

	off, _, err := s.idx.Read(-1)
	if err != nil {
		s.nextOff = baseOff
	} else {
		s.nextOff = baseOff + uint64(off) + 1
	}

	return s, nil
}
