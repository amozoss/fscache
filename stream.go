// Package stream provides a way to read and write to a synchronous buffered pipe, with multiple reader support.
package fscache

import (
	"errors"
	"sync"
)

// ErrRemoving is returned when requesting a Reader on a Stream which is being Removed.
var (
	ErrRemoving = errors.New("cannot open a new reader while removing file")
	NoWriter    = errors.New("No writer available, was close or never created")
)

// Stream has one writer and can have many readers
type Stream struct {
	name     string
	writer   *Writer
	grp      sync.WaitGroup
	fs       FileSystem
	removing bool
	mu       sync.Mutex // Used to sync removing and cnt
	cnt      int64      // keeps track of open streams, used for IsOpen
}

// Creates a new Stream with Name "name" in FileSystem fs.
func NewStream(name string, fs FileSystem) *Stream {
	sf := &Stream{
		name:     name,
		fs:       fs,
		removing: false,
	}
	return sf
}

// Assumes file is written
func (s *Stream) GetWriter() (*Writer, error) {
	if s.writer == nil {
		f, err := s.fs.Create(s.Name())
		if err != nil {
			return nil, err
		}
		s.writer = NewWriter(f, s.dec)
		s.inc()
	}
	return s.writer, nil
}

// Name returns the name of the underlying File in the FileSystem.
func (s *Stream) Name() string {
	return s.name
}

func (s *Stream) IsOpen() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.cnt > 0
}

func (s *Stream) Size() (int64, error) {
	return s.fs.Size(s.Name())
}

// Remove will block until the Stream and all its Readers have been Closed,
// at which point it will delete the underlying file. NextReader() will return
// ErrRemoving if called after Remove.
func (s *Stream) Remove() error {
	s.mu.Lock()
	s.removing = true
	s.mu.Unlock()
	s.grp.Wait()
	return s.fs.Remove(s.Name())
}

func (s *Stream) isRemoving() bool {
	s.mu.Lock()
	defer func() {
		s.mu.Unlock()
	}()
	return s.removing
}

// NextReader will return a concurrent-safe Reader for this stream. Each Reader will
// see a complete and independent view of the file, and can Read while the stream
// is written to.
func (s *Stream) NextReader() (*Reader, error) {
	if s.isRemoving() {
		return nil, ErrRemoving
	}
	s.inc()

	file, err := s.fs.Open(s.Name())
	if err != nil {
		s.dec()
		return nil, err
	}

	return NewReader(file, s.writer, s.dec), nil
}

func (s *Stream) inc() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cnt += 1
	s.grp.Add(1)
}

func (s *Stream) dec() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cnt -= 1
	s.grp.Done()
}
