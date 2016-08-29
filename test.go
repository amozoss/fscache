package fscache

import (
	"bytes"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
)

type Test struct {
	t   *testing.T
	dir string
}

func Wrap(t *testing.T, dir string) *Test {
	test := &Test{
		t: t,
	}

	dir, err := ioutil.TempDir("", dir)
	test.AssertNoError(err)
	test.dir = dir
	return test
}

func (t *Test) AssertError(err error) {
	if err == nil {
		t.t.Fatal(errors.New(fmt.Sprintf("expected error: %s", err)))
	}
}

func (t *Test) AssertNoError(err error) {
	if err != nil {
		t.t.Fatal(errors.New(fmt.Sprintf("expected no error: %s", err)))
	}
}

func (t *Test) Assert(cond bool, msg ...string) {
	if !cond {
		t.t.Fatal(errors.New(fmt.Sprintf("expected true: got %t %s", cond,
			handleMsg(msg))))
	}
}

func (t *Test) AssertByteEqual(p, q []byte) {
	t.Assert(bytes.Equal(p, q),
		fmt.Sprintf("expected %s, got %s", string(p), string(q)))
}

func handleMsg(msg []string) string {
	if msg == nil || len(msg) == 0 {
		return ""
	} else {
		return msg[0]
	}
}

func (t *Test) Dir() string {
	return t.dir
}

func (t *Test) CreateFile(name string) *os.File {
	f, err := os.OpenFile(filepath.Join(t.dir, name),
		os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
	t.AssertNoError(err)
	return f
}

func (t *Test) Close() {
	err := os.RemoveAll(t.dir)
	t.AssertNoError(err)
}
