package logger

import (
	"github.com/cnosdb/cnosdb/pkg/zaputil"

	"go.uber.org/zap/zapcore"
)

const (
	DefaultLogTimeFormat = "2006-01-02T15:04:05.000000Z07:00"
	// DefaultLogFormat is the default format of the log.
	DefaultLogFormat = "text"
	DefaultLogLevel  = zapcore.InfoLevel

	// DefaultLogMaxSize is the default size of log files.
	DefaultLogMaxSize = 300 // MB
)

type LogFileConfig struct {
	zaputil.FileConfig
}

// NewLogFileConfig "comment"
func NewLogFileConfig(maxSize uint) LogFileConfig {
	return LogFileConfig{
		FileConfig: zaputil.FileConfig{
			MaxSize: int(maxSize),
		},
	}
}

type Config struct {
	zaputil.Config
}

func NewDefaultLogConfig() *Config {
	return &Config{
		Config: zaputil.Config{
			Level:            DefaultLogLevel,
			Format:           DefaultLogFormat,
			DisableTimestamp: false,
			File:             zaputil.FileConfig{},
		},
	}
}

// NewLogConfig "comment"
func NewLogConfig(level zapcore.Level, format string, fileCfg LogFileConfig, disableTimestamp bool, opts ...func(*zaputil.Config)) *Config {
	c := &Config{
		Config: zaputil.Config{
			Level:            level,
			Format:           format,
			DisableTimestamp: disableTimestamp,
			File:             fileCfg.FileConfig,
		},
	}
	for _, opt := range opts {
		opt(&c.Config)
	}
	return c
}
