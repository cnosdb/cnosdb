package verifytombstone

import (
	"errors"
	"fmt"
	"github.com/cnosdb/db/tsdb/engine/tsm1"
	"github.com/spf13/cobra"
	"io"
	"os"
	"path/filepath"
	"time"
)

// Options represents the program execution for "influx_inspect verify-tombstone".
type Options struct {
	Stderr    io.Writer
	Stdout    io.Writer
	path      string
	verbosity int
}

// NewOptions returns a new instance of Command.
func NewOptions() *Options {
	return &Options{
		Stderr: os.Stderr,
		Stdout: os.Stdout,
	}
}

var opt = NewOptions()
var v bool
var vv bool
var vvv bool

func GetCommand() *cobra.Command {
	c := &cobra.Command{
		Use:   "verifytombstone",
		Short: "Verifies the integrity of tombstones.",
		Run: func(cmd *cobra.Command, args []string) {
			if err := verifytombstone(cmd, args); err != nil {
				fmt.Println(err)
			}
		},
	}
	c.PersistentFlags().StringVar(&opt.path, "path", os.Getenv("HOME")+"/.influxdb", "path to find tombstone files")
	c.PersistentFlags().BoolVar(&v, "v", false, "verbose: emit periodic progress")
	c.PersistentFlags().BoolVar(&vv, "vv", false, "very verbose: emit every tombstone entry key and time range")
	c.PersistentFlags().BoolVar(&vvv, "vvv", false, "very very verbose: emit every tombstone entry key and RFC3339Nano time range")
	return c
}

func verifytombstone(cmd *cobra.Command, args []string) error {
	runner := verifier{w: opt.Stdout}
	if v {
		runner.verbosity = verbose
	}
	if vv {
		runner.verbosity = veryVerbose
	}
	if vvv {
		runner.verbosity = veryVeryVerbose
	}

	return runner.Run()
}

const (
	quiet = iota
	verbose
	veryVerbose
	veryVeryVerbose
)

type verifier struct {
	path      string
	verbosity int

	w     io.Writer
	files []string
	f     string
}

func (v *verifier) loadFiles() error {
	return filepath.Walk(v.path, func(path string, f os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if filepath.Ext(path) == "."+tsm1.TombstoneFileExtension {
			v.files = append(v.files, path)
		}
		return nil
	})
}

func (v *verifier) Next() bool {
	if len(v.files) == 0 {
		return false
	}

	v.f, v.files = v.files[0], v.files[1:]
	return true
}

func (v *verifier) Run() error {
	if err := v.loadFiles(); err != nil {
		return err
	}

	var failed bool
	start := time.Now()
	for v.Next() {
		if v.verbosity > quiet {
			fmt.Fprintf(v.w, "Verifying: %q\n", v.f)
		}

		tombstoner := tsm1.NewTombstoner(v.f, nil)
		if !tombstoner.HasTombstones() {
			fmt.Fprintf(v.w, "%s has no tombstone entries", v.f)
			continue
		}

		var totalEntries int64
		err := tombstoner.Walk(func(t tsm1.Tombstone) error {
			totalEntries++
			if v.verbosity > quiet && totalEntries%(10*1e6) == 0 {
				fmt.Fprintf(v.w, "Verified %d tombstone entries\n", totalEntries)
			} else if v.verbosity > verbose {
				var min interface{} = t.Min
				var max interface{} = t.Max
				if v.verbosity > veryVerbose {
					min = time.Unix(0, t.Min)
					max = time.Unix(0, t.Max)
				}
				fmt.Printf("key: %q, min: %v, max: %v\n", t.Key, min, max)
			}
			return nil
		})
		if err != nil {
			fmt.Fprintf(v.w, "%q failed to walk tombstone entries: %v. Last okay entry: %d\n", v.f, err, totalEntries)
			failed = true
			continue
		}

		fmt.Fprintf(v.w, "Completed verification for %q in %v.\nVerified %d entries\n\n", v.f, time.Since(start), totalEntries)
	}

	if failed {
		return errors.New("failed tombstone verification")
	}
	return nil
}
