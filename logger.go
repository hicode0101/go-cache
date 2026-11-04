package gocache

import (
	"fmt"
	"io"
	"log"
	"os"
)

// Logger is the minimal logging interface used by the cache. It is
// intentionally small so that any structured logger (zap, slog, logrus,
// zerolog) can be adapted with a trivial wrapper.
type Logger interface {
	Debugf(format string, args ...any)
	Warnf(format string, args ...any)
	Errorf(format string, args ...any)
}

// NopLogger discards everything. It is the default logger, keeping the
// library silent out of the box.
var NopLogger Logger = nopLogger{}

type nopLogger struct{}

func (nopLogger) Debugf(string, ...any) {}
func (nopLogger) Warnf(string, ...any)  {}
func (nopLogger) Errorf(string, ...any) {}

// NewStdLogger adapts a standard library *log.Logger into the Logger
// interface. Passing nil writes to stderr, matching log.Default().
func NewStdLogger(l *log.Logger) Logger {
	if l == nil {
		l = log.New(os.Stderr, "gocache ", log.LstdFlags)
	}
	return stdLogger{l}
}

type stdLogger struct{ l *log.Logger }

func (s stdLogger) Debugf(format string, args ...any) { s.output("DEBUG ", format, args...) }
func (s stdLogger) Warnf(format string, args ...any)  { s.output("WARN  ", format, args...) }
func (s stdLogger) Errorf(format string, args ...any) { s.output("ERROR ", format, args...) }

func (s stdLogger) output(level, format string, args ...any) {
	s.l.Printf("%s%s", level, fmt.Sprintf(format, args...))
}

// NewWriterLogger writes log lines to w. Handy for tests and simple CLIs.
func NewWriterLogger(w io.Writer) Logger {
	return NewStdLogger(log.New(w, "gocache ", log.LstdFlags))
}
