package fscache

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"sync"
	"testing"
)

var (
	testdata = []byte("hello\nworld\n")
	errFail  = errors.New("fail")
)

func TestStream(t *testing.T) {
	test := NewStreamTest(t)
	defer test.Close()

	writer, err := test.stream.GetWriter()
	test.AssertNoError(err)
	_, err = writer.Write(nil)
	test.AssertNoError(err)

	errs := make(chan error, 10)
	var test_wg sync.WaitGroup

	test_wg.Add(10)
	for i := 0; i < 10; i++ {
		go func() {
			r, err := test.stream.NextReader()
			if err != nil {
				errs <- err
			}
			test.AssertReader(r, errs)
			test_wg.Done()
		}()
	}

	for i := 0; i < 10; i++ {
		writer.Write(testdata)
	}

	writer.Close()
	go func() {
		for err := range errs {
			test.AssertNoError(err)
		}
	}()
	test_wg.Wait()
}

func TestRemove(t *testing.T) {
	test := NewStreamTest(t)
	defer test.Close()
	var test_wg sync.WaitGroup
	test_wg.Add(1)
	go func() {
		test.stream.Remove()
		test_wg.Done()
	}()
	test_wg.Wait()
	_, err := test.stream.NextReader()
	test.Assert(err == ErrRemoving, "Expected error")
}

func (t *StreamTest) AssertReader(r ReaderAtCloser, errs chan error) {
	defer r.Close()

	buf := bytes.NewBuffer(nil)
	// should read part of the stream
	sr := io.NewSectionReader(r, 1+int64(len(testdata)*5), 5)
	_, err := io.Copy(buf, sr)
	if err != nil {
		errs <- err
		return
	}
	if !bytes.Equal(buf.Bytes(), testdata[1:6]) {
		errs <- errors.New(fmt.Sprintf("expected %s, got %s", string(buf.Bytes()),
			string(testdata[1:6])))
		return
	}

	buf.Reset()
	io.Copy(buf, r)
	p := bytes.Repeat(testdata, 10)
	q := buf.Bytes()
	if !bytes.Equal(p, q) {
		errs <- errors.New(fmt.Sprintf("expected %s, got %s", string(p), string(q)))
		return
	}
	errs <- nil
}

//////////////////////////////////////////////////////////////////////////
// Helpers
//////////////////////////////////////////////////////////////////////////
type StreamTest struct {
	*Test
	stream *Stream
}

func NewStreamTest(t *testing.T) *StreamTest {
	return &StreamTest{
		Test:   Wrap(t, "streamtest"),
		stream: NewStream("text.txt", NewMemFs()),
	}
}
