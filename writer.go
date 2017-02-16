package fscache

import (
	"errors"
	"sync"
)

type Writer struct {
	mu       sync.RWMutex
	closed   bool
	size     int64
	on_close func()
	cond     *sync.Cond
	file     WriteFile
}

func NewWriter(file WriteFile, on_close func()) *Writer {
	w := &Writer{
		closed:   false,
		on_close: on_close,

		file: file,
	}
	w.cond = sync.NewCond(w.mu.RLocker())
	return w
}

// Write writes p to the Stream. It's concurrent safe to be called with Stream's other methods.
func (w *Writer) Write(p []byte) (int, error) {
	w.mu.Lock()
	wrote, err := w.file.Write(p)
	if wrote > 0 {
		w.size += int64(wrote)
	}
	w.mu.Unlock()
	w.cond.Broadcast()
	return wrote, err
}

func (w *Writer) Wait(off int64) (n int64, open bool) {
	w.mu.RLock()
	defer w.mu.RUnlock()
	for !w.closed && off >= w.size {
		w.cond.Wait()
	}
	return w.size - off, !w.closed
}

// Must be read with RLock
func (w *Writer) IsOpen() bool {
	return !w.closed
}

// Close will close the writer. This will cause Readers to return EOF once
// they have read the entire stream.
func (w *Writer) Close() error {
	w.mu.Lock()
	if w.closed {
		return errors.New("stream already closed")
	}

	w.closed = true
	w.cond.Broadcast()
	w.mu.Unlock()
	defer w.on_close()
	return w.file.Close()
}
