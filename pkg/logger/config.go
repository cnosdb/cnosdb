package logger

import (
	"github.com/cnosdb/cnosdb/internal/log"
)

const (
	DefaultLogTimeFormat = "2006/01/02 15:04:05.000"
	// DefaultLogFormat is the default format of the log.
	DefaultLogFormat = "text"
	DefaultLogLevel  = "INFO"

	// DefaultLogMaxSize is the default size of log files.
	DefaultLogMaxSize = 300 // MB
)

type LogFileConfig struct {
	log.FileConfig
}

// NewLogFileConfig "comment"
func NewLogFileConfig(maxSize uint) LogFileConfig {
	return LogFileConfig{
		FileConfig: log.FileConfig{
			MaxSize: int(maxSize),
		},
	}
}

type Config struct {
	log.Config
}

func NewDefaultLogConfig() *Config {
	return &Config{
		Config: log.Config{
			Level:            DefaultLogLevel,
			Format:           DefaultLogFormat,
			DisableTimestamp: false,
			File:             log.FileConfig{},
		},
	}
}

// NewLogConfig "comment"
func NewLogConfig(level, format string, fileCfg LogFileConfig, disableTimestamp bool, opts ...func(*log.Config)) *Config {
	c := &Config{
		Config: log.Config{
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
