package fscache

import (
	"bytes"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func createFile(name string) (*os.File, error) {
	return os.OpenFile(name, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
}

func testDir(t *testing.T) string {
	dir, err := ioutil.TempDir("", "test-")
	if err != nil {
		t.Error(err.Error())
		t.Fail()
	}
	return dir
}

func testCaches(t *testing.T, run func(c Cache)) {
	dir := testDir(t)
	c, err := New(dir, 0700, 1*time.Hour)
	if err != nil {
		t.Error(err.Error())
		return
	}
	run(c)
}

func TestMemFs(t *testing.T) {
	fs := NewMemFs()
	if _, err := fs.Open("test"); err == nil {
		t.Errorf("stream shouldn't exist")
	}
	fs.Remove("test")

	f, err := fs.Create("test")
	if err != nil {
		t.Errorf("failed to create test")
	}
	f.Write([]byte("hello"))
	f.Close()

	r, err := fs.Open("test")
	if err != nil {
		t.Errorf("couldn't open test")
	}
	p, err := ioutil.ReadAll(r)
	r.Close()
	if !bytes.Equal(p, []byte("hello")) {
		t.Errorf("expected hello, got %s", string(p))
	}
}

func TestLoadCleanup(t *testing.T) {
	dir := testDir(t)
	name := "test"
	key := fileName(name)
	f, err := createFile(filepath.Join(dir, key))
	if err != nil {
		t.Error(err.Error())
	}
	f.Close()
	<-time.After(time.Second)
	f, err = createFile(filepath.Join(dir, key))
	if err != nil {
		t.Error(err.Error())
	}
	f.Close()

	c, err := New(dir, 0700, 0)
	if err != nil {
		t.Error(err.Error())
		return
	}
	defer c.Clean()

	if !c.Exists(name) {
		t.Errorf("expected test to exist")
	}
}

func TestReload(t *testing.T) {
	dir := testDir(t)
	c, err := New(dir, 0700, 0)
	if err != nil {
		t.Error(err.Error())
		return
	}
	r, w, err := c.Get("stream")
	if err != nil {
		t.Error(err.Error())
		return
	}
	r.Close()
	text := "hello world"
	w.Write([]byte(text))
	w.Close()

	nc, err := New(dir, 0700, 0)
	if err != nil {
		t.Error(err.Error())
		return
	}
	r, w, err = nc.Get("stream")
	if w != nil {
		t.Errorf("expected writer to be nil")
	}
	p, err := ioutil.ReadAll(r)
	if !bytes.Equal(p, []byte(text)) {
		t.Errorf("expected %s, got %s", text, string(p))
	}
	r.Close()

	defer nc.Clean()

	if !nc.Exists("stream") {
		t.Errorf("expected stream to be reloaded")
	} else {
		nc.Remove("stream")
		if nc.Exists("stream") {
			t.Errorf("expected stream to be removed")
		}
	}
}

func TestReaper(t *testing.T) {
	dir := testDir(t)
	fs, err := NewFs(dir, 0700)
	if err != nil {
		t.Error(err.Error())
		t.FailNow()
	}

	c, err := NewCache(fs, dir, NewReaper(0*time.Second, 100*time.Millisecond))

	if err != nil {
		t.Error(err.Error())
		return
	}
	defer c.Clean()

	r, w, err := c.Get("stream")
	w.Write([]byte("hello"))
	w.Close()
	io.Copy(ioutil.Discard, r)

	if !c.Exists("stream") {
		t.Errorf("stream should exist")
	}

	<-time.After(200 * time.Millisecond)

	if !c.Exists("stream") {
		t.Errorf("a file expired while in use, fail!")
	}
	r.Close()

	<-time.After(200 * time.Millisecond)

	if c.Exists("stream") {
		t.Errorf("stream should have been reaped")
	}

	files, err := ioutil.ReadDir(dir)
	if err != nil {
		t.Error(err.Error())
		return
	}

	if len(files) > 0 {
		t.Errorf("expected empty directory")
	}
}

func TestReaperNoExpire(t *testing.T) {
	testCaches(t, func(c Cache) {
		defer c.Clean()
		r, w, err := c.Get("stream")
		if err != nil {
			t.Error(err.Error())
			t.FailNow()
		}
		w.Write([]byte("hello"))
		w.Close()
		io.Copy(ioutil.Discard, r)
		r.Close()

		if !c.Exists("stream") {
			t.Errorf("stream should exist")
		}

		if lc, ok := c.(*cache); ok {
			lc.haunt()
			if !c.Exists("stream") {
				t.Errorf("stream shouldn't have been reaped")
			}
		}
	})
}

func TestSanity(t *testing.T) {
	testCaches(t, func(c Cache) {
		defer c.Clean()

		r, w, err := c.Get("looooooooooooooooooooooooooooong")
		if err != nil {
			t.Error(err.Error())
			return
		}
		defer r.Close()

		w.Write([]byte("hello world\n"))
		w.Close()

		buf := bytes.NewBuffer(nil)
		_, err = io.Copy(buf, r)
		if err != nil {
			t.Error(err.Error())
			return
		}
		if !bytes.Equal(buf.Bytes(), []byte("hello world\n")) {
			t.Errorf("unexpected output %s", buf.Bytes())
		}
	})
}

func TestConcurrent(t *testing.T) {
	testCaches(t, func(c Cache) {
		defer c.Clean()

		r, w, err := c.Get("stream")
		r.Close()
		if err != nil {
			t.Error(err.Error())
			return
		}
		go func() {
			w.Write([]byte("hello"))
			<-time.After(100 * time.Millisecond)
			w.Write([]byte("world"))
			w.Close()
		}()

		if c.Exists("stream") {
			r, _, err := c.Get("stream")
			if err != nil {
				t.Error(err.Error())
				return
			}
			buf := bytes.NewBuffer(nil)
			io.Copy(buf, r)
			r.Close()
			if !bytes.Equal(buf.Bytes(), []byte("helloworld")) {
				t.Errorf("unexpected output %s", buf.Bytes())
			}
		}
	})
}

func TestSize(t *testing.T) {
	testCaches(t, func(c Cache) {
		defer c.Clean()

		l := c.Size("dankmemes")
		if l != 0 {
			t.Error("nonexistant entry had nonzero length")
			return
		}

		r, w, err := c.Get("dankmemes")
		if err != nil {
			t.Error(err.Error())
			return
		}
		defer r.Close()

		w.Write([]byte("leroy jenkins"))
		w.Close()

		l = c.Size("dankmemes")
		if l != int64(len([]byte("leroy jenkins"))) {
			t.Errorf("unexpected entry length: %d", l)
			return
		}
	})
}
