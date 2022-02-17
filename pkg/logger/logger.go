package logger

import (
	"context"

	"github.com/cnosdb/cnosdb/internal/log"

	"github.com/pkg/errors"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// InitZapLogger initializes a zap logger with cfg.
func InitZapLogger(cfg *Config) error {
	gl, props, err := log.InitLogger(&cfg.Config, zap.AddStacktrace(zapcore.FatalLevel))
	if err != nil {
		return errors.Unwrap(err)
	}
	log.ReplaceGlobals(gl, props)

	return nil
}

// SetLevel sets the zap logger's level.
func SetLevel(level string) error {
	l := zap.NewAtomicLevel()
	if err := l.UnmarshalText([]byte(level)); err != nil {
		return errors.Unwrap(err)
	}
	log.SetLevel(l.Level())
	return nil
}

type ctxLogKeyType struct{}

var ctxLogKey = ctxLogKeyType{}

// Logger gets a contextual logger from current context.
// contextual logger will output common fields from context.
func Logger(ctx context.Context) *zap.Logger {
	if ctxlogger, ok := ctx.Value(ctxLogKey).(*zap.Logger); ok {
		return ctxlogger
	}
	return log.L()
}

// BgLogger is alias of `logutil.BgLogger()`
func BgLogger() *zap.Logger {
	return log.L()
}

// WithKeyValue attaches key/value to context.
func WithKeyValue(ctx context.Context, key, value string) context.Context {
	var logger *zap.Logger
	if ctxLogger, ok := ctx.Value(ctxLogKey).(*zap.Logger); ok {
		logger = ctxLogger
	} else {
		logger = log.L()
	}
	return context.WithValue(ctx, ctxLogKey, logger.With(zap.String(key, value)))
}
