package verifytombstone

import (
	"errors"
	"fmt"
	"github.com/cnosdb/cnosdb/vend/db/tsdb/engine/tsm1"
	"github.com/spf13/cobra"
	"io"
	"os"
	"path/filepath"
	"time"
)

// Options represents the program execution for "cnosdb_inspect verifytombstone".
type Options struct {
	Stderr    io.Writer
	Stdout    io.Writer
	path      string
	verbosity int
	v         bool
	vv        bool
	vvv       bool
	files     []string
	file      string
}

// NewOptions returns a new instance of Options.
func NewOptions() *Options {
	return &Options{
		Stderr: os.Stderr,
		Stdout: os.Stdout,
	}
}

var opt = NewOptions()

func GetCommand() *cobra.Command {
	c := &cobra.Command{
		Use:   "verifytombstone",
		Short: "Verifies the integrity of tombstones.",
		RunE: func(cmd *cobra.Command, args []string) error {
			opt.files = args
			if opt.v {
				opt.verbosity = verbose
			}
			if opt.vv {
				opt.verbosity = veryVerbose
			}
			if opt.vvv {
				opt.verbosity = veryVeryVerbose
			}
			return opt.Run()
		},
	}
	c.PersistentFlags().StringVar(&opt.path, "path", os.Getenv("HOME")+"/.cnosdb", "path to find tombstone files")
	c.PersistentFlags().BoolVar(&opt.v, "v", false, "verbose: emit periodic progress")
	c.PersistentFlags().BoolVar(&opt.vv, "vv", false, "very verbose: emit every tombstone entry key and time range")
	c.PersistentFlags().BoolVar(&opt.vvv, "vvv", false, "very very verbose: emit every tombstone entry key and RFC3339Nano time range")
	return c
}

const (
	quiet = iota
	verbose
	veryVerbose
	veryVeryVerbose
)

func (opt *Options) loadFiles() error {
	return filepath.Walk(opt.path, func(path string, f os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if filepath.Ext(path) == "."+tsm1.TombstoneFileExtension {
			opt.files = append(opt.files, path)
		}
		return nil
	})
}

func (opt *Options) Next() bool {
	if len(opt.files) == 0 {
		return false
	}

	opt.file, opt.files = opt.files[0], opt.files[1:]
	return true
}

func (opt *Options) Run() error {
	if err := opt.loadFiles(); err != nil {
		return err
	}

	var failed bool
	start := time.Now()
	for opt.Next() {
		if opt.verbosity > quiet {
			fmt.Fprintf(opt.Stdout, "Verifying: %q\n", opt.file)
		}
		tombstoner := tsm1.NewTombstoner(opt.file, nil)
		if !tombstoner.HasTombstones() {
			fmt.Fprintf(opt.Stdout, "%s has no tombstone entries", opt.file)
			continue
		}

		var totalEntries int64
		err := tombstoner.Walk(func(t tsm1.Tombstone) error {
			totalEntries++
			if opt.verbosity > quiet && totalEntries%(10*1e6) == 0 {
				fmt.Fprintf(opt.Stdout, "Verified %d tombstone entries\n", totalEntries)
			} else if opt.verbosity > verbose {
				var min interface{} = t.Min
				var max interface{} = t.Max
				if opt.verbosity > veryVerbose {
					min = time.Unix(0, t.Min)
					max = time.Unix(0, t.Max)
				}
				fmt.Printf("key: %q, min: %v, max: %v\n", t.Key, min, max)
			}
			return nil
		})
		if err != nil {
			fmt.Fprintf(opt.Stdout, "%q failed to walk tombstone entries: %v. Last okay entry: %d\n", opt.file, err, totalEntries)
			failed = true
			continue
		}

		fmt.Fprintf(opt.Stdout, "Completed verification for %q in %v.\nVerified %d entries\n\n", opt.file, time.Since(start), totalEntries)
	}

	if failed {
		return errors.New("failed tombstone verification")
	}
	return nil
}
