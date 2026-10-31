package cache

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/govengers/cache/internal/recovery"
)

type backgroundWrite struct {
	operation string
	write     func() error
	onFailure func()
}

// backgroundWriter runs writes one at a time, in the order they were queued, on one goroutine.
type backgroundWriter struct {
	timeout time.Duration
	logger  *slog.Logger
	queue   chan backgroundWrite
	closing chan struct{} // closed by close; wakes writes waiting for room
	done    chan struct{} // closed when the last queued write finished

	// stateMutex and senders keep the queue open until no enqueue can send to it any more.
	stateMutex sync.Mutex
	closed     bool
	senders    sync.WaitGroup
}

func startBackgroundWriter(queueSize int, timeout time.Duration, logger *slog.Logger) *backgroundWriter {
	writer := &backgroundWriter{
		timeout: timeout,
		logger:  logger,
		queue:   make(chan backgroundWrite, queueSize),
		closing: make(chan struct{}),
		done:    make(chan struct{}),
	}
	go writer.run() // perform recovers every write, so nothing in run can panic
	return writer
}

// enqueue queues write, detached from ctx's cancellation so it completes after the caller
// returned. onFailure, if any, runs when the write fails or panics.
func (w *backgroundWriter) enqueue(ctx context.Context, operation string, write func(ctx context.Context) error, onFailure func()) error {
	w.stateMutex.Lock()
	if w.closed {
		w.stateMutex.Unlock()
		return ErrClosed
	}
	w.senders.Add(1)
	w.stateMutex.Unlock()
	defer w.senders.Done()

	detached := context.WithoutCancel(ctx)
	queued := backgroundWrite{
		operation: operation,
		write: func() error {
			writeContext, cancel := context.WithTimeout(detached, w.timeout)
			defer cancel()
			return write(writeContext)
		},
		onFailure: onFailure,
	}
	select {
	case w.queue <- queued:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("cache: queue background %s: %w", operation, ctx.Err())
	case <-w.closing:
		return ErrClosed
	}
}

// close stops accepting writes and waits until the queued ones are done or ctx ends.
func (w *backgroundWriter) close(ctx context.Context) error {
	w.stateMutex.Lock()
	if !w.closed {
		w.closed = true
		close(w.closing)
		// Closing the queue while an enqueue may still send to it would panic; the senders
		// leave quickly once closing is closed.
		go func() {
			w.senders.Wait()
			close(w.queue)
		}()
	}
	w.stateMutex.Unlock()
	select {
	case <-w.done:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("cache: wait for background writes: %w", ctx.Err())
	}
}

func (w *backgroundWriter) run() {
	defer close(w.done)
	for queued := range w.queue {
		w.perform(queued)
	}
}

func (w *backgroundWriter) perform(queued backgroundWrite) {
	succeeded := false
	recovery.Run(w.logger, "cache: background "+queued.operation, func() {
		if err := queued.write(); err != nil {
			w.logger.Error("cache: background "+queued.operation+" failed", "error", err)
			return
		}
		succeeded = true
	})
	if !succeeded && queued.onFailure != nil {
		recovery.Run(w.logger, "cache: background "+queued.operation+" failure handling", queued.onFailure)
	}
}
