package proxy

import (
	"errors"
	"fmt"
	"net/http"
	"time"
)

// streamResponseWriter bounds every potentially blocking write and flush to a
// downstream streaming client. The standard net/http response writer supports
// write deadlines; ErrNotSupported is tolerated for compatible test writers.
type streamResponseWriter struct {
	writer     http.ResponseWriter
	controller *http.ResponseController
	timeout    time.Duration
}

func newStreamResponseWriter(writer http.ResponseWriter, timeout time.Duration) *streamResponseWriter {
	if timeout <= 0 {
		timeout = defaultReaderWriteTimeout
	}
	return &streamResponseWriter{
		writer:     writer,
		controller: http.NewResponseController(writer),
		timeout:    timeout,
	}
}

func (writer *streamResponseWriter) Write(payload []byte) (int, error) {
	if err := writer.refreshDeadline(); err != nil {
		return 0, err
	}
	return writer.writer.Write(payload)
}

func (writer *streamResponseWriter) Flush() error {
	if err := writer.refreshDeadline(); err != nil {
		return err
	}
	if err := writer.controller.Flush(); err != nil {
		return fmt.Errorf("flush downstream stream: %w", err)
	}
	return nil
}

func (writer *streamResponseWriter) clearDeadline() {
	_ = writer.controller.SetWriteDeadline(time.Time{})
}

func (writer *streamResponseWriter) refreshDeadline() error {
	err := writer.controller.SetWriteDeadline(time.Now().Add(writer.timeout))
	if errors.Is(err, http.ErrNotSupported) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("set downstream stream write deadline: %w", err)
	}
	return nil
}
