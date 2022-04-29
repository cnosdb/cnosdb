package utils

import (
	"context"

	"github.com/cnosdb/cnosdb/pkg/logger"

	"go.uber.org/zap"
)

// WithRecovery wraps goroutine startup call with force recovery.
// it will dump current goroutine stack into log if catch any recover result.
//   exec:      execute logic function.
//   recoverFn: handler will be called after recover and before dump stack, passing `nil` means noop.
func WithRecovery(exec func(), recoverFn func(r interface{})) {
	defer func() {
		r := recover()
		if recoverFn != nil {
			recoverFn(r)
		}
		if r != nil {
			logger.Error("panic in the recoverable goroutine",
				zap.Reflect("r", r),
				zap.Stack("stack trace"))
		}
	}()
	exec()
}

// HasCancelled checks whether context has be cancelled.
func HasCancelled(ctx context.Context) (cancel bool) {
	select {
	case <-ctx.Done():
		cancel = true
	default:
	}
	return
}
