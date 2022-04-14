package logger

import (
	"errors"
	"fmt"
	"io"
	"sync/atomic"
	"time"

	"github.com/cnosdb/cnosdb/pkg/zaputil"
	"github.com/mattn/go-isatty"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var _globalL, _globalP, _globalS atomic.Value

func InitZapLogger(cfg *Config) error {
	lg, prop, err := zaputil.InitLogger(&cfg.Config, zap.AddStacktrace(zapcore.FatalLevel), zap.Fields(zap.String("log_id", nextID())))
	if err != nil {
		return errors.Unwrap(err)
	}
	_globalL.Store(lg)
	_globalS.Store(lg.Sugar())
	_globalP.Store(prop)

	return nil
}

// NewLoggerWithWriter creates a new zap.Logger with an io.Writer
func NewLoggerWithWriter(w io.Writer, opts ...zap.Option) *zap.Logger {
	lg, _ := NewLoggerWithConfigAndWriter(nil, w, opts...)
	return lg
}

// NewLoggerWithConfigAndWriter creates a new zap.Logger with a Config and an io.Writer
func NewLoggerWithConfigAndWriter(config *Config, w io.Writer, opts ...zap.Option) (*zap.Logger, error) {
	var isTerminal = false
	if f, ok := w.(interface {
		Fd() uintptr
	}); ok {
		isTerminal = isatty.IsTerminal(f.Fd())
	}

	if config == nil {
		var cfg = NewDefaultLogConfig()
		if isTerminal {
			cfg.Format = "console"
		}
		lg, _, _ := zaputil.InitLoggerWithWriteSyncer(&cfg.Config, zapcore.Lock(zapcore.AddSync(w)), opts...)
		return lg, nil
	} else {
		lg, _, err := zaputil.InitLoggerWithWriteSyncer(&config.Config, zapcore.Lock(zapcore.AddSync(w)), opts...)
		if err != nil {
			return nil, err
		}
		return lg, nil
	}
}

// L returns the global Logger, which can be reconfigured with ReplaceGlobals.
// It's safe for concurrent use.
func L() *zap.Logger {
	return _globalL.Load().(*zap.Logger)
}

// S returns the global SugaredLogger, which can be reconfigured with
// ReplaceGlobals. It's safe for concurrent use.
func S() *zap.SugaredLogger {
	return _globalS.Load().(*zap.SugaredLogger)
}

func p() *zaputil.ZapProperties {
	return _globalP.Load().(*zaputil.ZapProperties)
}

// Debug logs a message at DebugLevel. The message includes any fields passed
// at the log site, as well as any fields accumulated on the logger.
func Debug(msg string, fields ...zap.Field) {
	L().WithOptions(zap.AddCallerSkip(1)).Debug(msg, fields...)
}

// Info logs a message at InfoLevel. The message includes any fields passed
// at the log site, as well as any fields accumulated on the logger.
func Info(msg string, fields ...zap.Field) {
	L().WithOptions(zap.AddCallerSkip(1)).Info(msg, fields...)
}

// Warn logs a message at WarnLevel. The message includes any fields passed
// at the log site, as well as any fields accumulated on the logger.
func Warn(msg string, fields ...zap.Field) {
	L().WithOptions(zap.AddCallerSkip(1)).Warn(msg, fields...)
}

// Error logs a message at ErrorLevel. The message includes any fields passed
// at the log site, as well as any fields accumulated on the logger.
func Error(msg string, fields ...zap.Field) {
	L().WithOptions(zap.AddCallerSkip(1)).Error(msg, fields...)
}

// Panic logs a message at PanicLevel. The message includes any fields passed
// at the log site, as well as any fields accumulated on the logger.
//
// The logger then panics, even if logging at PanicLevel is disabled.
func Panic(msg string, fields ...zap.Field) {
	L().WithOptions(zap.AddCallerSkip(1)).Panic(msg, fields...)
}

// Fatal logs a message at FatalLevel. The message includes any fields passed
// at the log site, as well as any fields accumulated on the logger.
//
// The logger then calls os.Exit(1), even if logging at FatalLevel is
// disabled.
func Fatal(msg string, fields ...zap.Field) {
	L().WithOptions(zap.AddCallerSkip(1)).Fatal(msg, fields...)
}

// With creates a child logger and adds structured context to it.
// Fields added to the child don't affect the parent, and vice versa.
func With(fields ...zap.Field) *zap.Logger {
	return L().WithOptions(zap.AddCallerSkip(1)).With(fields...)
}

const (
	year = 365 * 24 * time.Hour
	week = 7 * 24 * time.Hour
	day  = 24 * time.Hour
)

// DurationLiteral represents a duration literal from a key  and time duration.
func DurationLiteral(key string, val time.Duration) zapcore.Field {
	if val == 0 {
		return zap.String(key, "0s")
	}

	var (
		value int
		unit  string
	)
	switch {
	case val%year == 0:
		value = int(val / year)
		unit = "y"
	case val%week == 0:
		value = int(val / week)
		unit = "w"
	case val%day == 0:
		value = int(val / day)
		unit = "d"
	case val%time.Hour == 0:
		value = int(val / time.Hour)
		unit = "h"
	case val%time.Minute == 0:
		value = int(val / time.Minute)
		unit = "m"
	case val%time.Second == 0:
		value = int(val / time.Second)
		unit = "s"
	default:
		value = int(val / time.Millisecond)
		unit = "ms"
	}
	return zap.String(key, fmt.Sprintf("%d%s", value, unit))
}
