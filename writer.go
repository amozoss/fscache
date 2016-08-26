package fscache

import (
	"errors"
	"fmt"
	"sync"
)

type Writer struct {
	mu       sync.RWMutex
	closed   bool
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
	defer w.cond.Broadcast()
	w.mu.Lock()
	defer w.mu.Unlock()
	fmt.Printf("Writing %s\n", string(p))
	return w.file.Write(p)
}

func (w *Writer) Wait() {
	w.cond.Wait()
}

// Must be read with RLock
func (w *Writer) IsOpen() bool {
	return !w.closed
}

func (w *Writer) RLock() {
	w.mu.RLock()
}

func (w *Writer) RUnlock() {
	w.mu.RUnlock()
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
	fmt.Println("Broadcast()")
	w.mu.Unlock()
	defer w.on_close()
	return w.file.Close()
}
