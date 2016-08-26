package fscache

import (
	"io"
	"os"
	"time"

	"gopkg.in/djherbis/atime.v1"
)

// FileSystem is used as the source for a Cache.
type FileSystem interface {
	Create(name string) (File, error)
	Open(name string) (File, error)
	Remove(name string) error
	// It will be used to check expiry of a file, and must be concurrent safe
	// with modifications to the FileSystem (writes, reads etc.)
	AccessTimes(name string) (rt, wt time.Time, err error)
	Size(name string) (int64, error)
}

type File interface {
	Name() string
	io.Writer
	io.ReaderAt
	io.Reader
	io.Closer
}

type WriteFile interface {
	Name() string  // The name used to Create/Open the File
	io.WriteCloser // Concurrent writing must be supported.
}

type ReadFile interface {
	Name() string // The name used to Create/Open the File
	io.ReaderAt   // Similarly to Reader
	io.ReadCloser
}

type stdFs struct{}

// NewFs returns a FileSystem rooted at directory dir.
// Dir is created with perms if it doesn't exist.
func NewFs(dir string, mode os.FileMode) (FileSystem, error) {
	return &stdFs{}, os.MkdirAll(dir, mode)
}

func (fs *stdFs) Create(name string) (File, error) {
	return os.Create(name)
}

func (fs *stdFs) Open(name string) (File, error) {
	return os.Open(name)
}

func (fs *stdFs) Remove(name string) error {
	return os.Remove(name)
}

func (fs *stdFs) AccessTimes(name string) (rt, wt time.Time, err error) {
	fi, err := os.Stat(name)
	if err != nil {
		return rt, wt, err
	}
	return atime.Get(fi), fi.ModTime(), nil
}

func (fs *stdFs) Size(name string) (size int64, err error) {
	fi, err := os.Stat(name)
	if err != nil {
		return 0, err
	}
	return fi.Size(), nil
}
