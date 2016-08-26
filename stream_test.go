package fscache

import (
	"bytes"
	"errors"
	"io"
	"os"
	"testing"
	"time"
)

var (
	testdata = []byte("hello\nworld\n")
	errFail  = errors.New("fail")
)

type badFs struct {
	readers []File
}
type badFile struct{ name string }

func (r badFile) Name() string                            { return r.name }
func (r badFile) Read(p []byte) (int, error)              { return 0, errFail }
func (r badFile) ReadAt(p []byte, off int64) (int, error) { return 0, errFail }
func (r badFile) Write(p []byte) (int, error)             { return 0, errFail }
func (r badFile) Close() error                            { return errFail }

func (fs badFs) Create(name string) (File, error) { return os.Create(name) }
func (fs badFs) Open(name string) (File, error) {
	if len(fs.readers) > 0 {
		f := fs.readers[len(fs.readers)-1]
		fs.readers = fs.readers[:len(fs.readers)-1]
		return f, nil
	}
	return nil, errFail
}
func (fs badFs) Remove(name string) error { return os.Remove(name) }
func (fs badFs) AccessTimes(name string) (rt, wt time.Time, err error) {
	return rt, wt, nil
}
func (r badFs) Size(name string) (int64, error) { return 0, errFail }

func TestBadFile(t *testing.T) {
	fs := badFs{readers: make([]File, 0, 1)}
	fs.readers = append(fs.readers, badFile{name: "test"})
	s := NewStream("test", fs)
	defer s.Remove()

	r, err := s.NextReader()
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	defer r.Close()
	if r.Name() != "test" {
		t.Errorf("expected name to to be 'test' got %s", r.Name())
		t.FailNow()
	}
	if _, err := r.ReadAt(nil, 0); err == nil {
		t.Error("expected ReadAt error")
		t.FailNow()
	}
	if _, err := r.Read(nil); err == nil {
		t.Error("expected Read error")
		t.FailNow()
	}
}

func TestBadFs(t *testing.T) {
	s := NewStream("test", badFs{})
	defer s.Remove()

	r, err := s.NextReader()
	if err == nil {
		t.Error("expected open error")
		t.FailNow()
	} else {
		return
	}
	r.Close()
}

func TestMem(t *testing.T) {
	s := NewStream("test.txt", NewMemFs())
	writer, err := s.GetWriter()
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	writer.Write(nil)
	testFile(s, t)
}

func TestRemove(t *testing.T) {
	s := NewStream("test.txt", NewMemFs())
	go s.Remove()
	<-time.After(100 * time.Millisecond)
	r, err := s.NextReader()
	switch err {
	case ErrRemoving:
	case nil:
		t.Error("expected error on NextReader()")
		r.Close()
	default:
		t.Error("expected diff error on NextReader()", err)
	}

}

func testFile(s *Stream, t *testing.T) {
	for i := 0; i < 10; i++ {
		go testReader(s, t)
	}
	writer, err := s.GetWriter()
	if err != nil {
		t.Error(err)
		t.FailNow()
	}

	for i := 0; i < 10; i++ {
		writer.Write(testdata)
		<-time.After(10 * time.Millisecond)
	}

	writer.Close()
	testReader(s, t)
	s.Remove()
}

func testReader(f *Stream, t *testing.T) {
	r, err := f.NextReader()
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	defer r.Close()

	buf := bytes.NewBuffer(nil)
	sr := io.NewSectionReader(r, 1+int64(len(testdata)*5), 5)
	io.Copy(buf, sr)
	if !bytes.Equal(buf.Bytes(), testdata[1:6]) {
		t.Errorf("unequal %s", buf.Bytes())
		return
	}

	buf.Reset()
	io.Copy(buf, r)
	if !bytes.Equal(buf.Bytes(), bytes.Repeat(testdata, 10)) {
		t.Errorf("unequal %s", buf.Bytes())
		return
	}
}
