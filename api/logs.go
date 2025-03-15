package api

import (
	"net/http"
	"time"

	"github.com/go-chi/chi/v5/middleware"
	"github.com/rs/zerolog/log"
)

type LogEntry struct {
	method string
	path   string
}

func (l *LogEntry) Write(status, bytes int, header http.Header, elapsed time.Duration, extra interface{}) {
	log.Info().Str("method", l.method).Str("path", l.path).Int("status", status).Int("bytes", bytes).
		Dur("elapsed_ms", elapsed).
		Interface("extra", extra).
		Msg("Request completed")
}

func (l *LogEntry) Panic(v interface{}, stack []byte) {
	log.Error().Interface("panic", v).Bytes("stack", stack).Msg("Panic occurred")
}

type LogFormatter struct{}

func (f *LogFormatter) NewLogEntry(r *http.Request) middleware.LogEntry {
	// log.Info().Str("method", r.Method).Str("path", r.URL.Path).Msg("Request started")
	return &LogEntry{
		method: r.Method,
		path:   r.URL.Path,
	}
}
