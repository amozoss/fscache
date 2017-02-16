package fscache

import "io"

type CacheReader interface {
	Name() string
	io.ReaderAt
	io.Reader
	io.Closer
}

// Reader is a concurrent-safe Stream Reader.
type Reader struct {
	writer   *Writer // writer can be nil if file was already written
	on_close func()
	file     ReadFile
	read_off int64
}

func NewReader(file ReadFile, writer *Writer, on_close func()) *Reader {
	return &Reader{
		writer:   writer,
		on_close: on_close,
		file:     file,
	}
}

// Name returns the name of the underlying File in the FileSystem.
func (r *Reader) Name() string { return r.file.Name() }

// ReadAt blocks while waiting for the requested section of the Stream to
// be written, unless the Stream is closed in which case it will always
// return immediately.
func (r *Reader) ReadAt(p []byte, off int64) (n int, err error) {
	if r.writer == nil {
		return r.file.ReadAt(p, off)
	}

	var m int = 0
	for {
		m, err = r.file.ReadAt(p[n:], off)
		n += m
		off += int64(m)

		switch {
		case n != 0 && err == nil:
			return n, err
		case err == io.EOF:
			if v, open := r.writer.Wait(off); v == 0 && !open {
				return n, io.EOF
			}
		case err != nil:
			return n, err
		}
	}
}

// Read reads from the Stream. If the end of an open Stream is reached, Read
// blocks until more data is written or the Stream is Closed.
func (r *Reader) Read(p []byte) (n int, err error) {
	if r.writer == nil {
		return r.file.Read(p)
	}

	var m int
	for {
		m, err = r.file.Read(p[n:])
		n += m
		r.read_off += int64(m)

		switch {
		case n != 0 && err == nil:
			return n, nil
		case err == io.EOF:
			if v, open := r.writer.Wait(r.read_off); v == 0 && !open {
				return n, io.EOF
			}
		case err != nil:
			return n, err
		}
	}
}

// Close closes this Reader on the Stream. This must be called when done with the
// Reader or else the Stream cannot be Removed.
func (r *Reader) Close() error {
	defer r.on_close()
	return r.file.Close()
}
