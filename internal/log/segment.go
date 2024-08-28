package log

import (
	"fmt"
	"os"
	"path"

	api "github.com/n0tB0b17/distri/api/v1"
	"google.golang.org/protobuf/proto"
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
		path.Join(dir, fmt.Sprintf("%d%s", baseOff, ".store")),
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
		path.Join(dir, fmt.Sprintf("%d%s", baseOff, ".index")),
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

func (S *segment) Append(R *api.Record) (uint64, error) {
	cur := S.nextOff
	R.Offset = cur

	b, err := proto.Marshal(R)
	if err != nil {
		fmt.Println("[SEGMENT | APPEND] > error while marshalling given record", R.Value)
		return 0, err
	}

	_, pos, err := S.store.Append(b)
	if err != nil {
		fmt.Println("[SEGMENT | APPEND] > error while appending marshalled record to store", err.Error())
		return 0, err
	}

	if err := S.idx.Write(
		uint32(S.nextOff-uint64(S.baseOff)), pos,
	); err != nil {
		return 0, err
	}

	S.nextOff++
	return cur, nil
}

func (S *segment) Read(offset uint64) (*api.Record, error) {
	_, pos, err := S.idx.Read(int64(offset - S.baseOff))
	if err != nil {
		fmt.Println("[SEGMENT | READ > error while reading index for given offset  < ", offset)
		return nil, err
	}

	b, err := S.store.Read(pos)
	if err != nil {
		fmt.Println("[SEGMENT | READ] > error while reading records for given index")
		return nil, err
	}

	R := &api.Record{}
	proto.Unmarshal(b, R)
	return R, nil
}

func (S *segment) Remove() error {
	if err := S.Close(); err != nil {
		fmt.Println("[SEGMENT | REMOVE] > ")
		return err
	}

	if err := os.Remove(S.idx.Name()); err != nil {
		fmt.Println("")
		return err
	}

	if err := os.Remove(S.store.Name()); err != nil {
		fmt.Println("")
		return err
	}

	return nil
}

func (S *segment) Close() error {
	if err := S.idx.Close(); err != nil {
		fmt.Println("[SEGMENT | CLOSE] > error while closing index")
		return err
	}

	if err := S.store.Close(); err != nil {
		fmt.Println("[SEGMENT | CLOSE] > error while closing store")
		return err
	}

	return nil
}

func (S *segment) IsMaxed() bool {
	return S.idx.size > S.config.Segment.MaxIndexBytes ||
		S.store.size > S.config.Segment.MaxStoreBytes
}

func nearestMultiple(j, k uint64) uint64 {
	if j >= 0 {
		return (j / k) * k
	} else {
		return ((j - k + 1) / k) * k
	}
}
