package server

import (
	"fmt"
	"sync"
)

type Record struct {
	Value  []byte `json:"Value"`
	Offset uint64 `json:"Offset"`
}

type Log struct {
	mu      sync.Mutex
	records []Record
}

func NewLog() *Log {
	return &Log{}
}

// nice way to add logs
func (L *Log) append(R Record) (uint64, error) {
	L.mu.Lock()
	defer L.mu.Unlock()
	R.Offset = uint64(len(L.records))
	L.records = append(L.records, R)
	return R.Offset, nil
}

func (L *Log) get(offset uint64) (Record, error) {
	L.mu.Lock()
	defer L.mu.Unlock()

	if offset >= uint64(len(L.records)) {
		return Record{}, fmt.Errorf("offset is wayyyy offf")
	}

	return L.records[offset], nil
}
