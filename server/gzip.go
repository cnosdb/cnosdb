package server

import (
	"compress/gzip"
	"io"
	"net/http"
	"sync"
)

type lazyGzipResponseWriter struct {
	io.Writer
	http.ResponseWriter
	http.Flusher
	http.CloseNotifier
	wroteHeader bool
}

func (w *lazyGzipResponseWriter) WriteHeader(code int) {
	if w.wroteHeader {
		return
	}

	w.wroteHeader = true
	if code == http.StatusOK {
		w.Header().Set("Content-Encoding", "gzip")
		// Add gzip compressor
		if _, ok := w.Writer.(*gzip.Writer); !ok {
			w.Writer = getGzipWriter(w.Writer)
		}
	}

	w.ResponseWriter.WriteHeader(code)
}

func (w *lazyGzipResponseWriter) Write(p []byte) (int, error) {
	if !w.wroteHeader {
		w.WriteHeader(http.StatusOK)
	}
	return w.Writer.Write(p)
}

func (w *lazyGzipResponseWriter) Flush() {
	// Flush writer, if supported
	if f, ok := w.Writer.(interface {
		Flush()
	}); ok {
		f.Flush()
	}

	// Flush the HTTP response
	if w.Flusher != nil {
		w.Flusher.Flush()
	}
}

func (w *lazyGzipResponseWriter) Close() error {
	if gw, ok := w.Writer.(*gzip.Writer); ok {
		putGzipWriter(gw)
	}

	return nil
}

var gzipWriterPool = sync.Pool{
	New: func() interface{} {
		return gzip.NewWriter(nil)
	},
}

func getGzipWriter(w io.Writer) *gzip.Writer {
	gz := gzipWriterPool.Get().(*gzip.Writer)
	gz.Reset(w)
	return gz
}

func putGzipWriter(gz *gzip.Writer) {
	_ = gz.Close()
	gzipWriterPool.Put(gz)
}
