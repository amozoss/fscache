package fscache

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"sync"
	"testing"
	"time"
)

func TestLoad(t *testing.T) {
	test := Wrap(t, "fscache")
	defer test.Close()
	name := "test"
	key := fileName(name)

	f := test.CreateFile(key)
	to_write := []byte("hello world")
	err := ioutil.WriteFile(f.Name(), to_write, 0700)
	test.AssertNoError(err)
	f.Close()

	cache, err := New(test.Dir(), 0700, time.Second)
	test.AssertNoError(err)
	test.Assert(cache.Exists(name), fmt.Sprintf("expected %s to exist",
		name))

	r, w, err := cache.Get(name, int64(len(to_write)))
	test.AssertNoError(err)
	test.Assert(w == nil, "writer should be nil")

	p, err := ioutil.ReadAll(r)
	test.AssertNoError(err)
	test.AssertByteEqual(to_write, p)
	r.Close()
}

func TestReload(t *testing.T) {
	test := Wrap(t, "fscache")
	defer test.Close()
	cache, err := New(test.Dir(), 0700, time.Second)
	test.AssertNoError(err)

	text := []byte("hello world")
	r, w, err := cache.Get("stream", int64(len(text)))
	test.AssertNoError(err)
	err = r.Close()
	test.AssertNoError(err)
	_, err = w.Write(text)
	test.AssertNoError(err)
	err = w.Close()
	test.AssertNoError(err)

	cache, err = New(test.Dir(), 0700, time.Second)
	test.AssertNoError(err)
	r, w, err = cache.Get("stream", int64(len(text)))
	test.AssertNoError(err)
	test.Assert(w == nil, "expected writer to be nil")

	p, err := ioutil.ReadAll(r)
	test.AssertNoError(err)
	test.AssertByteEqual(text, p)
	r.Close()

	test.Assert(cache.Exists("stream"), "expected stream to be reloaded")
	cache.Remove("stream")
	test.Assert(!cache.Exists("stream"), "expected stream to be removed")
}

func TestReaper(t *testing.T) {
	reap_interval := time.Second
	test := NewMemFsCacheTest(t, reap_interval)
	defer test.Close()

	test.SetNow(2016, time.September, 1, 0, 0, 0, 0)
	to_write := []byte("hello")
	r, w, err := test.cache.Get("stream", int64(len(to_write)))
	n := test.AssertWrite(w, to_write)
	test.AssertRead(r, n)

	test.cache.reap(reap_interval)
	test.Assert(test.cache.Exists("stream"), "stream should exist")

	test.SetNow(2016, time.September, 1, 0, 0, 2, 0)
	test.cache.reap(reap_interval)
	test.Assert(test.cache.Exists("stream"), "a file expired while in use, fail!")
	r.Close()

	test.SetNow(2016, time.September, 1, 0, 0, 4, 0)
	test.cache.reap(reap_interval)
	test.Assert(!test.cache.Exists("stream"), "stream should have been reaped")
	files, err := ioutil.ReadDir(test.Dir())
	test.AssertNoError(err)

	test.Assert(len(files) == 0, "expected empty directory")
}

func TestReaperNoExpire(t *testing.T) {
	reap_interval := 0 * time.Second
	test := NewMemFsCacheTest(t, reap_interval)
	defer test.Close()

	test.SetNow(2016, time.September, 1, 0, 0, 0, 0)
	to_write := []byte("hello")
	r, w, err := test.cache.Get("stream", int64(len(to_write)))
	test.AssertNoError(err)
	n := test.AssertWrite(w, to_write)
	test.AssertRead(r, n)
	test.cache.reap(reap_interval)

	test.Assert(test.cache.Exists("stream"), "stream should exist")

	test.SetNow(2017, time.September, 1, 0, 0, 0, 0)
	test.cache.reap(reap_interval)
	test.Assert(test.cache.Exists("stream"), "stream should exist")
}

func TestWrongSize(t *testing.T) {
	test := NewFsCacheTest(t)
	defer test.Close()
	to_write := []byte("hello")
	r, w, err := test.cache.Get("file", int64(len(to_write)))
	test.AssertNoError(err)
	r.Close()

	test.AssertWrite(w, to_write)

	to_write = []byte("hello world")
	r, w, err = test.cache.Get("file", int64(len(to_write)))
	test.AssertNoError(err)
	test.Assert(w != nil, "writer should not be nil")
	defer r.Close()
	test.AssertWrite(w, to_write)

	buf := bytes.NewBuffer(nil)
	_, err = io.Copy(buf, r)
	test.AssertNoError(err)

	test.AssertByteEqual(to_write, buf.Bytes())
}

func TestSanity(t *testing.T) {
	test := NewFsCacheTest(t)
	defer test.Close()
	to_write := []byte("hello")
	r, w, err := test.cache.Get("looooooooooooooooooooooooooooong",
		int64(len(to_write)))
	test.AssertNoError(err)
	defer r.Close()

	test.AssertWrite(w, to_write)

	buf := bytes.NewBuffer(nil)
	_, err = io.Copy(buf, r)
	test.AssertNoError(err)

	test.AssertByteEqual(to_write, buf.Bytes())
}

func TestConcurrent(t *testing.T) {
	test := NewFsCacheTest(t)
	defer test.Close()

	r, w, err := test.cache.Get("stream", 10)
	test.AssertNoError(err)
	err = r.Close()
	test.AssertNoError(err)

	var test_wg sync.WaitGroup
	test_wg.Add(1)
	go func() {
		w.Write([]byte("hello"))
		test_wg.Done()
		w.Write([]byte("world"))
		w.Close()
	}()

	test_wg.Wait()

	test.Assert(test.cache.Exists("stream"))
	r, w, err = test.cache.Get("stream", 10)
	test.AssertNoError(err)
	test.Assert(w == nil, "writer should be nil")

	buf := bytes.NewBuffer(nil)
	_, err = io.Copy(buf, r)
	test.AssertNoError(err)
	err = r.Close()
	test.AssertNoError(err)
	test.AssertByteEqual([]byte("helloworld"), buf.Bytes())
}

func TestSize(t *testing.T) {
	test := NewFsCacheTest(t)
	defer test.Close()

	_, err := test.cache.Size("dankmemes")
	test.AssertError(err)

	to_write := []byte("leroy jenkins")
	r, w, err := test.cache.Get("dankmemes", int64(len(to_write)))
	test.AssertNoError(err)
	defer r.Close()

	test.AssertWrite(w, to_write)

	l, err := test.cache.Size("dankmemes")
	test.AssertNoError(err)
	test.Assert(l == int64(len(to_write)),
		fmt.Sprintf("expected: %d, got: %d", len(to_write), l))
}

////////////////////////////////////////////////////////////////////////////
// Helpers
////////////////////////////////////////////////////////////////////////////

type FsCacheTest struct {
	*Test
	cache             *FsCache
	original_now_hook func() time.Time
}

func NewFsCacheTest(t *testing.T) *FsCacheTest {
	test := Wrap(t, "fstest")
	c, err := New(test.Dir(), 0700, 1*time.Hour)
	test.AssertNoError(err)
	return &FsCacheTest{
		Test:              test,
		cache:             c,
		original_now_hook: nowHook,
	}
}

func NewMemFsCacheTest(t *testing.T, expiry time.Duration) *FsCacheTest {
	test := Wrap(t, "fstest")
	fs := NewMemFs()
	c, err := NewCache(test.Dir(), fs, expiry)
	test.AssertNoError(err)
	return &FsCacheTest{
		Test:              test,
		cache:             c,
		original_now_hook: nowHook,
	}
}

func (t *FsCacheTest) AssertWrite(w io.WriteCloser, p []byte) int {
	n, err := w.Write(p)
	t.AssertNoError(err)
	err = w.Close()
	t.AssertNoError(err)
	return n
}

func (t *FsCacheTest) AssertRead(r ReaderAtCloser, n int) {
	written, err := io.Copy(ioutil.Discard, r)
	t.AssertNoError(err)
	t.Assert(int64(n) == written,
		fmt.Sprintf("expected: %d, got: %d", n, written))
}

func (t *FsCacheTest) SetNow(year int, month time.Month, day, hour, min, sec,
	nsec int) {
	nowHook = func() time.Time {
		return time.Date(year, month, day, hour, min, sec, nsec, time.UTC)
	}
}

func (t *FsCacheTest) Close() {
	t.Test.Close()
	nowHook = t.original_now_hook
}
