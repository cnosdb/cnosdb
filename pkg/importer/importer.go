package importer

import "fmt"

// TODO complete importer

type Config struct {
	Path string
}

func NewConfig() *Config {
	return &Config{}
}

type Importer struct {
	config *Config
}

func NewImporter(c Config) *Importer {
	return &Importer{
		config: &c,
	}
}

func (i *Importer) Import() error {
	fmt.Printf("import '%s'\n", i.config.Path)
	return nil
}
