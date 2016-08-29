package fscache

import (
	"io/ioutil"
	"testing"
)

func TestMemFs(t *testing.T) {
	test := Wrap(t, "memfs")
	defer test.Close()
	fs := NewMemFs()
	file := "testfile"
	_, err := fs.Open(file)
	test.AssertError(err)
	fs.Remove(file)

	f, err := fs.Create(file)
	test.AssertNoError(err)
	to_write := []byte("hello")
	f.Write(to_write)
	f.Close()

	r, err := fs.Open(file)
	test.AssertNoError(err)
	p, err := ioutil.ReadAll(r)
	r.Close()
	test.AssertByteEqual(to_write, p)
}
