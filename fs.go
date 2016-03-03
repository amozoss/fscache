package fscache

import (
	"io/ioutil"
	"os"
	"time"

	"gopkg.in/djherbis/atime.v1"
	"github.com/amozoss/stream"
)

// FileSystem is used as the source for a Cache.
type FileSystem interface {
	// Stream FileSystem
	stream.FileSystem

	// Reload should look through the FileSystem and call the suplied fn
	// with the key/filename pairs that are found.
	Reload(func(key string)) error

	// RemoveAll should empty the FileSystem of all files.
	RemoveAll() error

	// AccessTimes takes a File.Name() and returns the last time the file was read,
	// and the last time it was written to.
	// It will be used to check expiry of a file, and must be concurrent safe
	// with modifications to the FileSystem (writes, reads etc.)
	AccessTimes(name string) (rt, wt time.Time, err error)
}

type stdFs struct {
	root string
}

// NewFs returns a FileSystem rooted at directory dir.
// Dir is created with perms if it doesn't exist.
func NewFs(dir string, mode os.FileMode) (FileSystem, error) {
	return &stdFs{root: dir}, os.MkdirAll(dir, mode)
}

func (fs *stdFs) Reload(add func(key string)) error {
	files, err := ioutil.ReadDir(fs.root)
	if err != nil {
		return err
	}

	for _, f := range files {
		// TODO Check expire time and remove old files
		add(f.Name())
		return nil
	}
	return nil
}

func (fs *stdFs) Create(name string) (stream.File, error) {
	return os.Create(name)
}

func (fs *stdFs) Open(name string) (stream.File, error) {
	return os.Open(name)
}

func (fs *stdFs) Remove(name string) error {
	return os.Remove(name)
}

func (fs *stdFs) RemoveAll() error {
	return os.RemoveAll(fs.root)
}

func (fs *stdFs) AccessTimes(name string) (rt, wt time.Time, err error) {
	fi, err := os.Stat(name)
	if err != nil {
		return rt, wt, err
	}
	return atime.Get(fi), fi.ModTime(), nil
}
