package fscache

import (
	"crypto/md5"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/amozoss/stream"
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

	// Clean will empty the cache and delete the cache folder.
	// Clean is not safe to call while streams are being read/written.
	Clean() error
}

type cache struct {
	mu    sync.RWMutex
	files map[string]*cachedFile
	grim  Reaper
	fs    FileSystem
	dir   string
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
		files: make(map[string]*cachedFile),
		grim:  grim,
		fs:    fs,
		dir:   dir,
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

	for key, f := range c.files {
		if atomic.LoadInt64(&f.cnt) > 0 {
			continue
		}

		lastRead, lastWrite, err := c.fs.AccessTimes(f.stream.Name())
		if err != nil {
			continue
		}

		if c.grim.Reap(key, lastRead, lastWrite) {
			delete(c.files, key)
			c.fs.Remove(f.stream.Name())
		}
	}
	return
}

func (c *cache) load() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.fs.Reload(func(key string) {
		// keys will be names of the files
		c.files[key] = c.oldFile(key)
	})
}

func (c *cache) Exists(name string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	_, ok := c.getFile(name)
	return ok
}

func fileName(name string) string {
	md5sum := md5.Sum([]byte(name))
	return fmt.Sprintf("%x", md5sum[:])
}

func (c *cache) putFile(name string, file *cachedFile) {
	key := fileName(name)
	c.files[key] = file
}

func (c *cache) getFile(name string) (*cachedFile, bool) {
	key := fileName(name)
	f, ok := c.files[key]
	return f, ok
}

func (c *cache) Get(name string) (r ReaderAtCloser, w io.WriteCloser, err error) {
	c.mu.RLock()
	f, ok := c.getFile(name)
	if ok {
		r, err = f.next()
		c.mu.RUnlock()
		return r, nil, err
	}
	c.mu.RUnlock()

	c.mu.Lock()
	defer c.mu.Unlock()

	f, ok = c.getFile(name)
	if ok {
		r, err = f.next()
		return r, nil, err
	}

	f, err = c.newFile(name)
	if err != nil {
		return nil, nil, err
	}

	r, err = f.next()
	if err != nil {
		f.Close()
		c.fs.Remove(f.stream.Name())
		return nil, nil, err
	}

	c.putFile(name, f)

	return r, f, err
}

func (c *cache) Remove(name string) error {
	c.mu.Lock()
	key := fileName(name)
	f, ok := c.getFile(name)
	if ok {
		delete(c.files, key)
	}
	c.mu.Unlock()

	if ok {
		return f.stream.Remove()
	}
	return nil
}

func (c *cache) Clean() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.files = make(map[string]*cachedFile)
	return c.fs.RemoveAll()
}

type cachedFile struct {
	stream *stream.Stream
	cnt    int64
}

func (c *cache) getPath(name string) string {
	// TODO root path
	return filepath.Join(c.dir, name)
}

func (c *cache) newFile(name string) (*cachedFile, error) {
	s, err := stream.NewStream(c.getPath(fileName(name)), c.fs)
	if err != nil {
		return nil, err
	}
	cf := &cachedFile{
		stream: s,
	}
	atomic.AddInt64(&cf.cnt, 1)
	return cf, nil
}

func (c *cache) oldFile(name string) *cachedFile {
	s, _ := stream.OldStream(c.getPath(name), c.fs)
	s.Close()
	cf := &cachedFile{
		stream: s,
	}
	return cf
}

func (f *cachedFile) next() (r ReaderAtCloser, err error) {
	reader, err := f.stream.NextReader()
	if err != nil {
		return nil, err
	}
	atomic.AddInt64(&f.cnt, 1)
	return &cacheReader{
		r: reader,
		cnt: &f.cnt,
	}, nil
}

func (f *cachedFile) Write(p []byte) (int, error) {
	return f.stream.Write(p)
}

func (f *cachedFile) Close() error {
	defer func() { atomic.AddInt64(&f.cnt, -1) }()
	return f.stream.Close()
}

type cacheReader struct {
	r   ReaderAtCloser
	cnt *int64
}

func (r *cacheReader) ReadAt(p []byte, off int64) (n int, err error) {
	return r.r.ReadAt(p, off)
}

func (r *cacheReader) Read(p []byte) (n int, err error) {
	return r.r.Read(p)
}

func (r *cacheReader) Close() error {
	defer func() { atomic.AddInt64(r.cnt, -1) }()
	return r.r.Close()
}
