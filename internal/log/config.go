package log

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const (
	defaultLogMaxSize = 300 // MB
)

// FileConfig "comment"
type FileConfig struct {
	// Log filename, leave empty to disable file log.
	Filename string `toml:"filename"`
	// Max size for a single file, in MB.
	MaxSize int `toml:"max-size"`
	// Max log keep days, default is never deleting.
	MaxDays int `toml:"max-days"`
	// Maximum number of old log files to retain.
	MaxBackups int `toml:"max-backups"`
}

// Config "comment"
type Config struct {
	// Log level.
	Level string `toml:"level"`
	// Log format. one of json, text, or console.
	Format string `toml:"format"`
	// Disable automatic timestamps in output.
	DisableTimestamp bool `toml:"disable-timestamp"`
	// File log config.
	File FileConfig `toml:"file"`
	// Development puts the logger in development mode, which changes the
	// behavior of DPanicLevel and takes stacktraces more liberally.
	Development bool `toml:"development"`
	// DisableCaller stops annotating logs with the calling function's file
	// name and line number. By default, all logs are annotated.
	DisableCaller bool `toml:"disable-caller"`
	// DisableStacktrace completely disables automatic stacktrace capturing. By
	// default, stacktraces are captured for WarnLevel and above logs in
	// development and ErrorLevel and above in production.
	DisableStacktrace bool `toml:"disable-stacktrace"`
	// DisableErrorVerbose stops annotating logs with the full verbose error
	// message.
	DisableErrorVerbose bool `toml:"disable-error-verbose"`
}

func newZapTextEncoder(cfg *Config) zapcore.Encoder {
	return NewTextEncoder(cfg)
}

func (cfg *Config) buildOptions(errSink zapcore.WriteSyncer) []zap.Option {
	opts := []zap.Option{zap.ErrorOutput(errSink)}

	if cfg.Development {
		opts = append(opts, zap.Development())
	}

	if !cfg.DisableCaller {
		opts = append(opts, zap.AddCaller())
	}

	stackLevel := zap.ErrorLevel
	if cfg.Development {
		stackLevel = zap.WarnLevel
	}
	if !cfg.DisableStacktrace {
		opts = append(opts, zap.AddStacktrace(stackLevel))
	}

	return opts
}
