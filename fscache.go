package fscache

import (
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

var logger = spacelog.GetLogger()

type cache struct {
	mu      sync.RWMutex
	streams map[string]*Stream
	grim    Reaper
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
func New(dir string, perms os.FileMode, expiry time.Duration) (Cache, error) {
	fs, err := NewFs(dir, perms)
	if err != nil {
		return nil, err
	}
	var grim Reaper
	if expiry > 0 {
		grim = &reaper{
			expiry: expiry,
			period: expiry,
		}
	}
	return NewCache(fs, dir, grim)
}

// NewCache creates a new Cache based on FileSystem fs.
// fs.Files() are loaded using the name they were created with as a key.
// Reaper is used to determine when files expire, nil means never expire.
func NewCache(fs FileSystem, dir string, grim Reaper) (Cache, error) {
	c := &cache{
		streams: make(map[string]*Stream),
		grim:    grim,
		fs:      fs,
		root:    dir,
	}
	err := c.load()
	if err != nil {
		return nil, err
	}
	if grim != nil {
		c.haunter()
	}
	return c, nil
}

func (c *cache) haunter() {
	c.haunt()
	time.AfterFunc(c.grim.Next(), c.haunter)
}

func (c *cache) haunt() {
	c.mu.Lock()
	defer c.mu.Unlock()

	for key, s := range c.streams {
		if s.IsOpen() {
			continue
		}

		lastRead, lastWrite, err := c.fs.AccessTimes(s.Name())
		if err != nil {
			continue
		}

		if c.grim.Reap(key, lastRead, lastWrite) {
			delete(c.streams, key)
			s.Remove()
		}
	}
	return
}

func (c *cache) load() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	files, err := ioutil.ReadDir(c.root)
	if err != nil {
		return err
	}

	for _, f := range files {
		// TODO Check expire time and remove old files
		key := f.Name()
		s, err := c.openStream(key)
		if err != nil {
			logger.Errorf("error opening stream %s : %v", key, err)
			err = c.Remove(key)
			logger.Errorf("error removing stream %s : %v", key, err)
			continue
		}
		c.streams[key] = s
	}
	return nil
}

func (c *cache) createStream(name string) (*Stream, error) {
	s, err := CreateStream(c.getPath(fileName(name)), c.fs)
	if err != nil {
		return nil, err
	}
	return s, nil
}

// When server starts up again, cached files are read in and reused
func (c *cache) openStream(name string) (*Stream, error) {
	s, err := OpenStream(c.getPath(name), c.fs)
	if err != nil {
		return nil, err
	}
	// Closing because it isn't being written to, might be a better way
	err = s.Close()
	if err != nil {
		return nil, err
	}
	return s, nil
}

func (c *cache) Exists(name string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	_, ok := c.getStream(name)
	return ok
}

func (c *cache) Size(name string) (int64, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
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

func (c *cache) putStream(name string, s *Stream) {
	key := fileName(name)
	c.streams[key] = s
}

func (c *cache) getStream(name string) (*Stream, bool) {
	key := fileName(name)
	f, ok := c.streams[key]
	return f, ok
}

func (c *cache) Get(name string) (r ReaderAtCloser, w io.WriteCloser, err error) {
	c.mu.RLock()
	s, ok := c.getStream(name)
	if ok {
		r, err = s.NextReader()
		c.mu.RUnlock()
		return r, nil, err
	}
	c.mu.RUnlock()

	c.mu.Lock()
	defer c.mu.Unlock()

	s, err = c.createStream(name)
	if err != nil {
		return nil, nil, err
	}

	r, err = s.NextReader()
	if err != nil {
		s.Close()
		s.Remove()
		return nil, nil, err
	}

	c.putStream(name, s)

	return r, s, err
}

func (c *cache) Remove(name string) error {
	c.mu.Lock()
	key := fileName(name)
	s, ok := c.getStream(name)
	if ok {
		delete(c.streams, key)
	}
	c.mu.Unlock()

	if ok {
		return s.Remove()
	}
	return nil
}

func (c *cache) Clean() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.streams = make(map[string]*Stream)
	return os.RemoveAll(c.root)
}

func (c *cache) getPath(name string) string {
	return filepath.Join(c.root, name)
}
