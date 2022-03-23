package options

import "os"

type options struct {
	ConfigFile string
	PidFile    string
	CpuProfile string
	MemProfile string
}

var Env = options{}

// GetConfigPath returns the config path from the options.
// It will return a path by searching in this order:
//   1. The CLI option in ConfigPath
//   2. The environment variable CNOSDB_META_CONFIG_PATH
//   3. The first cnosdb-meta.conf file on the path:
//        - ~/.cnosdb
//        - /etc/cnosdb
func (opt options) GetConfigPath() string {
	if opt.ConfigFile != "" {
		if opt.ConfigFile == os.DevNull {
			return ""
		}
		return opt.ConfigFile
	} else if envVar := os.Getenv("CNOSDB_META_CONFIG_PATH"); envVar != "" {
		return envVar
	}

	for _, path := range []string{
		os.ExpandEnv("${HOME}/.cnosdb/cnosdb-meta.conf"),
		"/etc/cnosdb/cnosdb-meta.conf",
	} {
		if _, err := os.Stat(path); err == nil {
			return path
		}
	}
	return ""
}
