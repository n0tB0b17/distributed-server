package log

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"os"
	"sync"
)

const (
	lenWidth = 8
)

type Store struct {
	*os.File
	mu   sync.Mutex
	size uint64
	buf  *bufio.Writer
}

func NewStore(f *os.File) (*Store, error) {
	fi, err := os.Stat(f.Name())
	if err != nil {
		fmt.Println("[STORE] > Error while creating new store", err.Error())
		return &Store{}, err
	}

	size := uint64(fi.Size())
	return &Store{
		size: size,
		buf:  bufio.NewWriter(f),
		File: f,
	}, nil
}

func (S *Store) Append(p []byte) (n uint64, index uint64, err error) {
	S.mu.Lock()
	defer S.mu.Unlock()
	index = S.size // position
	if err := binary.Write(S.buf, binary.BigEndian, uint64(len(p))); err != nil {
		fmt.Println("[STORE::APPEND] > Error while adding length to buffer", err.Error())
		return 0, 0, err
	}

	w, err := S.buf.Write(p) // adding segments
	if err != nil {
		fmt.Println("[STORE::APPEND] > Error while adding to store", err.Error())
		return 0, 0, err
	}

	// caluclating position of segment
	w += lenWidth
	S.size += uint64(w) // file-size value increased by 8 & pos value is updated.

	return uint64(w), index, nil
}

func (S *Store) Read(index uint64) ([]byte, error) {
	S.mu.Lock()
	defer S.mu.Unlock()

	if err := S.buf.Flush(); err != nil {
		fmt.Println("[STORE::READ] > Error while flushing buffer", err.Error())
		return nil, err
	}

	size := make([]byte, lenWidth)
	_, err := S.File.ReadAt(size, int64(index))
	if err != nil {
		fmt.Printf("[STORE::READ] > Error while reading from file: {%s}, at index: {%d}, for total size of {%d} \n", S.File.Name(), index, size)
		return nil, err
	}

	b := make([]byte, binary.BigEndian.Uint64(size))
	_, err = S.File.ReadAt(b, int64(index+lenWidth))
	if err != nil {
		fmt.Printf("[STORE::READ] > Error while reading from file: {%s}, at index: {%d}, for total size of {%d} \n", S.File.Name(), index+lenWidth, b)
		return nil, err
	}

	return b, nil
}

func (S *Store) ReadAt(p []byte, offset int64) (int, error) {
	S.mu.Lock()
	defer S.mu.Unlock()

	if err := S.buf.Flush(); err != nil {
		fmt.Println("[STORE::READ_AT] > Error while flusing buffer", err.Error())
		return 0, err
	}

	return S.File.ReadAt(p, offset)
}

func (S *Store) Close() error {
	S.mu.Lock()
	defer S.mu.Unlock()
	if err := S.buf.Flush(); err != nil {
		fmt.Println("[STORE::CLOSE] > Error while flusing buffer", err.Error())
		return err
	}

	return S.File.Close()
}
