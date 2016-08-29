package fscache

import (
	"context"
	"crypto/md5"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/spacemonkeygo/spacelog"
)

var (
	logger  = spacelog.GetLogger()
	nowHook = time.Now // used for testing
)

// Cache works like a concurrent-safe map for streams.
type Cache interface {

	// Get manages access to the streams in the cache.
	// If the key does not exist, w != nil and you can start writing to the stream.
	// If the key does exist, w == nil.
	// r will always be non-nil as long as err == nil and you must close r when you're done reading.
	// Get can be called concurrently, and writing and reading is concurrent safe.
	Get(name string) (ReaderAtCloser, io.WriteCloser, error)

	// Remove deletes the stream from the cache, blocking until the underlying
	// file can be deleted (all active streams finish with it).
	// It is safe to call Remove concurrently with Get.
	Remove(name string) error

	// Exists checks if a key is in the cache.
	// It is safe to call Exists concurrently with Get.
	Exists(name string) bool

	// Size returns the size of the stream on disk.
	// It is safe to use concurrently with Get.
	Size(name string) (int64, error)

	// Clean will empty the cache and delete the cache folder.
	// Clean is not safe to call while streams are being read/written.
	Clean() error
}

type FsCache struct {
	mu      sync.RWMutex
	streams map[string]*Stream
	fs      FileSystem
	root    string
}

type ReaderAtCloser interface {
	io.ReadCloser
	io.ReaderAt
}

// New creates a new Cache using NewFs(dir, perms).
// expiry is the duration after which an un-accessed key will be removed from
// the cache, a zero value expiro means never expire.
func New(dir string, perms os.FileMode, expiry time.Duration) (*FsCache,
	error) {
	fs, err := NewFs(dir, perms)
	if err != nil {
		return nil, err
	}
	return NewCache(dir, fs, expiry)
}

// NewCache creates a new Cache based on FileSystem fs.
// fs.Files() are loaded using the name they were created with as a key.
func NewCache(dir string, fs FileSystem, expiry time.Duration) (*FsCache,
	error) {
	c := &FsCache{
		streams: make(map[string]*Stream),
		fs:      fs,
		root:    dir,
	}
	err := c.load()
	if err != nil {
		return nil, err
	}
	if expiry > 0 {
		ctx := context.Background()
		go c.ReapEvery(ctx, expiry)
	}
	return c, nil
}

func (c *FsCache) load() error {
	files, err := ioutil.ReadDir(c.root)
	if err != nil {
		return err
	}

	for _, f := range files {
		// TODO Check expire time and remove old files
		key := f.Name()
		s := NewStream(c.getPath(key), c.fs)
		c.putKeyStream(key, s)
	}
	return nil
}

func (c *FsCache) Exists(name string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	_, ok := c.getStream(name)
	return ok
}

func (c *FsCache) Size(name string) (int64, error) {
	s, ok := c.getStream(name)
	if !ok {
		return 0, errors.New("file not found")
	}
	size, err := s.Size()
	if err != nil {
		return 0, err
	}
	return size, nil
}

func fileName(name string) string {
	md5sum := md5.Sum([]byte(name))
	return fmt.Sprintf("%x", md5sum[:])
}

func (c *FsCache) putKeyStream(key string, s *Stream) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.streams[key] = s
}

func (c *FsCache) putStream(name string, s *Stream) {
	key := fileName(name)
	c.putKeyStream(key, s)
}

func (c *FsCache) deleteStream(key string, lock bool) error {
	if lock {
		c.mu.Lock()
	}
	s, ok := c.streams[key]
	if ok {
		delete(c.streams, key)
	}
	if lock {
		c.mu.Unlock()
	}
	if ok {
		return s.Remove()
	}
	return nil
}

func (c *FsCache) getStream(name string) (*Stream, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	key := fileName(name)
	f, ok := c.streams[key]
	return f, ok
}

func (c *FsCache) createStream(name string) *Stream {
	key := fileName(name)
	s := NewStream(c.getPath(key), c.fs)
	c.putStream(name, s)
	return s
}

func (c *FsCache) Get(name string) (r ReaderAtCloser, w io.WriteCloser, err error) {
	s, ok := c.getStream(name)
	if ok {
		r, err := s.NextReader()

		return r, nil, err
	}

	s = c.createStream(name)
	writer, err := s.GetWriter()
	if err != nil {
		return nil, nil, err
	}

	r, err = s.NextReader()
	if err != nil {
		writer.Close()
		s.Remove()
		return nil, nil, err
	}

	return r, writer, err
}

func (c *FsCache) Remove(name string) error {
	key := fileName(name)
	return c.deleteStream(key, true)
}

func (c *FsCache) Clean() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.streams = make(map[string]*Stream)
	return os.RemoveAll(c.root)
}

func (c *FsCache) getPath(name string) string {
	return filepath.Join(c.root, name)
}

func (c *FsCache) ReapEvery(ctx context.Context, reap_interval time.Duration) {
	ticker := time.NewTicker(reap_interval)
	defer ticker.Stop()
	done := ctx.Done()
	for {
		select {
		case <-ticker.C:
			c.reap(reap_interval)
		case <-done:
			return
		}
	}
}

func (c *FsCache) reap(reap_interval time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for key, s := range c.streams {
		if s.IsOpen() {
			continue
		}

		lastRead, _, err := c.fs.AccessTimes(s.Name())
		if err != nil {
			logger.Error(err)
			continue
		}

		if lastRead.Before(nowHook().Add(-reap_interval)) {
			err = c.deleteStream(key, false)
			if err != nil {
				logger.Error(err)
				continue
			}
		}
	}
	return
}
