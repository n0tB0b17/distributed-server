package log

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"

	"github.com/tysonmote/gommap"
)

const (
	offSetWidth = 4
	indexWidth  = 8
	entWidth    = offSetWidth + indexWidth
)

type Index struct {
	file *os.File
	size uint64
	mmap gommap.MMap
}

func NewIndex(f *os.File, c Config) (*Index, error) {
	idx := &Index{
		file: f,
	}

	fi, err := os.Stat(f.Name())
	if err != nil {
		fmt.Println("[INDEX] > Error while validating file", err.Error())
		return &Index{}, err
	}

	idx.size = uint64(fi.Size())
	err = os.Truncate(f.Name(), int64(c.Segment.MaxIndexBytes))
	if err != nil {
		fmt.Println("[INDEX] > Error while truncating file", err.Error())
		return &Index{}, err
	}

	idx.mmap, err = gommap.Map(
		idx.file.Fd(),
		gommap.PROT_READ|gommap.PROT_WRITE,
		gommap.MAP_SHARED,
	)

	if err != nil {
		fmt.Println("[INDEX > Error while creating new mapping in virtual space", err.Error())
		return &Index{}, nil
	}
	return idx, nil
}

func (I *Index) Close() error {
	if err := I.mmap.Sync(gommap.MS_ASYNC); err != nil {
		fmt.Println("[CLOSING | INDEX] > Error while syncing mmap instance", err.Error())
		return err
	}

	if err := I.file.Sync(); err != nil {
		fmt.Println("[CLOSING | INDEX] > Error while closing given index file", err.Error())
		return err
	}

	if err := I.file.Truncate(int64(I.size)); err != nil {
		fmt.Println("[CLOSING | INDEX] > Error while truncating index file", err.Error())
		return err
	}

	return I.file.Close()
}

// for given offset, returns a position of a record in store
func (I *Index) Read(offset int64) (off uint32, idx uint64, err error) {
	if I.size == 0 {
		return 0, 0, io.EOF
	}

	if offset == -1 {
		off = uint32((I.size / entWidth) - 1)
	} else {
		off = uint32(offset)
	}

	idx = uint64(off) * entWidth
	if I.size < idx+entWidth {
		return 0, 0, io.EOF
	}

	off = binary.LittleEndian.Uint32(I.mmap[idx : idx+offSetWidth])
	idx = binary.LittleEndian.Uint64(I.mmap[idx+offSetWidth : idx+entWidth])
	return off, idx, nil
}

// add offset and position to index
func (I *Index) Write(off uint32, idx uint64) error {
	// validate if we have enough space to write
	if uint64(len(I.mmap)) < I.size+entWidth {
		return io.EOF
	}

	// encode both argument and write to memory-mapped file
	binary.LittleEndian.PutUint32(I.mmap[I.size:I.size+offSetWidth], off)
	binary.LittleEndian.PutUint64(I.mmap[I.size+offSetWidth:I.size+entWidth], idx)

	// increment position where next write happens
	I.size += uint64(entWidth)
	return nil
}

func (I *Index) Name() string {
	return I.file.Name()
}
