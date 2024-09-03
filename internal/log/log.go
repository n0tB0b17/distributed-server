package log

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"

	api "github.com/n0tB0b17/distri/api/v1"
)

type Log struct {
	mu     sync.Mutex
	dir    string
	config Config

	activeSegment *segment
	segments      []*segment
}

type originReader struct {
	*Store
	off int64
}

func NewLog(dir string, c Config) (*Log, error) {
	if c.Segment.MaxStoreBytes == 0 {
		c.Segment.MaxStoreBytes = 1024
	}

	if c.Segment.MaxIndexBytes == 0 {
		c.Segment.MaxIndexBytes = 1024
	}

	l := &Log{
		dir:    dir,
		config: c,
	}
	return l, l.setup()
}

func (L *Log) setup() error {
	var baseOffset []uint64
	files, err := ioutil.ReadDir(L.dir)
	if err != nil {
		fmt.Println("[LOG | SETUP] > Error while reading content of logs directory", err.Error())
		return err
	}

	for _, file := range files {
		offStr := strings.TrimPrefix(
			L.dir,
			path.Ext(file.Name()),
		)

		off, _ := strconv.ParseUint(offStr, 10, 0)
		baseOffset = append(baseOffset, off)
	}

	sort.Slice(baseOffset, func(i, j int) bool {
		return baseOffset[i] < baseOffset[j]
	})

	for i := 0; i < len(baseOffset); i++ {
		if err := L.newSegment(baseOffset[i]); err != nil {
			return err
		}

		i++
	}

	if L.segments == nil {
		if err = L.newSegment(L.config.Segment.InitialOffset); err != nil {
			return err
		}
	}
	return nil
}

func (L *Log) Append(R *api.Record) (uint64, error) {
	L.mu.Lock()
	defer L.mu.Unlock()
	offset, err := L.activeSegment.Append(R)
	if err != nil {
		fmt.Println("[LOG | APPEND] > Error while adding record to segment", err.Error())
		return 0, err
	}

	if L.activeSegment.IsMaxed() {
		L.newSegment(offset + 1)
	}
	return offset, nil
}

func (L *Log) Read(offset uint64) (*api.Record, error) {
	L.mu.Lock()
	defer L.mu.Unlock()
	var s *segment

	for _, segment := range L.segments {
		if segment.baseOff <= offset && offset < segment.nextOff {
			s = segment
			break
		}
	}

	if s == nil || s.nextOff <= offset {
		return nil, fmt.Errorf("offset out of range: %d", offset)
	}

	return s.Read(offset)
}

func (L *Log) Close() error {
	L.mu.Lock()
	defer L.mu.Unlock()

	for _, segment := range L.segments {
		if err := segment.Close(); err != nil {
			return err
		}
	}
	return nil
}

func (L *Log) Remove() error {
	if err := L.Close(); err != nil {
		return err
	}

	return os.RemoveAll(L.dir)
}

func (L *Log) Reset() error {
	if err := L.Remove(); err != nil {
		return err
	}

	return L.setup()
}

func (L *Log) newSegment(offset uint64) error {
	s, err := NewSegment(L.dir, offset, L.config)
	if err != nil {
		fmt.Println("[LOG | NEWSEGMENT] > error while generating new segment inside of log", err.Error())
		return err
	}

	L.segments = append(L.segments, s)
	L.activeSegment = s
	return nil
}

func (L *Log) LowestOffset() (uint64, error) {
	L.mu.Lock()
	defer L.mu.Unlock()

	return L.segments[0].baseOff, nil
}

func (L *Log) HighestOffset() (uint64, error) {
	L.mu.Lock()
	defer L.mu.Unlock()
	off := L.segments[len(L.segments)-1].baseOff
	if off == 0 {
		return 0, nil
	}

	return off - 1, nil
}

// remove segment whose highest offset is lower than lowest
// for storage mgmt
func (L *Log) Truncate(lowest uint64) error {
	L.mu.Lock()
	defer L.mu.Unlock()

	var segments []*segment

	for _, segment := range L.segments {
		if segment.nextOff <= lowest+1 {
			if err := segment.Remove(); err != nil {
				return err
			}

			continue
		}

		segments = append(segments, segment)
	}

	L.segments = segments
	return nil
}

func (L *Log) Reader() io.Reader {
	L.mu.Lock()
	defer L.mu.Unlock()

	readers := make([]io.Reader, len(L.segments))
	for i, segment := range L.segments {
		readers[i] = &originReader{Store: segment.store, off: 0}
	}

	return io.MultiReader(readers...)
}

func (OR *originReader) Read(p []byte) (int, error) {
	n, err := OR.ReadAt(p, OR.off)
	if err != nil {
		fmt.Println("[ORGIN_READER | READ] > error while reading", err.Error())
		return 0, err
	}

	OR.off += int64(n)
	return n, err
}
