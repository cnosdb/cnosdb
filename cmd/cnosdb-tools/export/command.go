package export

import (
	"compress/gzip"
	"errors"
	"fmt"
	"github.com/spf13/cobra"
	"io"
	"math"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/cnosdb/cnosdb/cmd/cnosdb-tools/internal/format"
	"github.com/cnosdb/cnosdb/cmd/cnosdb-tools/internal/format/binary"
	"github.com/cnosdb/cnosdb/cmd/cnosdb-tools/internal/format/line"
	"github.com/cnosdb/cnosdb/cmd/cnosdb-tools/internal/format/text"
	"github.com/cnosdb/cnosdb/cmd/cnosdb-tools/server"

	"go.uber.org/zap"
)

var (
	_ line.Writer
	_ binary.Writer
)

var opt = NewOption(server.NewSingleServer())

// Options represents the program execution for "store query".
type Options struct {
	// Standard input/output, overridden for testing.
	Stderr io.Writer
	Stdout io.Writer
	Logger *zap.Logger
	server server.Interface

	conflicts io.WriteCloser

	configPath    string
	database      string
	rp            string
	shardDuration time.Duration
	format        string
	r             rangeValue
	conflictPath  string
	ignore        bool
	print         bool
}

func GetCommand() *cobra.Command {
	c := &cobra.Command{
		Use:   "export",
		Short: "the export tool transforms existing shards to a new shard duration in order to consolidate into fewer shards.",

		RunE: func(cmd *cobra.Command, args []string) (err error) {
			//The error process is very naive before!

			if opt.database == "" {
				return errors.New("database is required")
			}

			switch opt.format {
			case "line", "binary", "series", "values", "discard":
			default:
				return fmt.Errorf("invalid format '%s'", opt.format)
			}

			if opt.conflictPath == "" && !opt.ignore {
				return errors.New("missing conflict-path")
			}

			if err != nil {
				return err
			}

			err = opt.server.Open(opt.configPath)

			if err != nil {
				return err
			}

			defer opt.server.Close()

			e, err := opt.openExporter()
			if err != nil {
				return err
			}
			defer e.Close()

			e.PrintPlan(opt.Stderr)

			if opt.print {
				return nil
			}

			if !opt.ignore {
				if f, err := os.Create(opt.conflictPath); err != nil {
					return err
				} else {
					opt.conflicts = gzip.NewWriter(f)
					defer func() {
						opt.conflicts.Close()
						f.Close()
					}()
				}
			}

			var wr format.Writer
			switch opt.format {
			case "line":
				wr = line.NewWriter(opt.Stdout)
			case "binary":
				wr = binary.NewWriter(opt.Stdout, opt.database, opt.rp, opt.shardDuration)
			case "series":
				wr = text.NewWriter(opt.Stdout, text.Series)
			case "values":
				wr = text.NewWriter(opt.Stdout, text.Values)
			case "discard":
				wr = format.Discard
			}
			defer func() {
				err = wr.Close()
			}()

			if opt.conflicts != nil {
				wr = format.NewConflictWriter(wr, line.NewWriter(opt.conflicts))
			} else {
				wr = format.NewConflictWriter(wr, format.DevNull)
			}

			return e.WriteTo(wr)
		},
	}

	c.PersistentFlags().StringVar(&opt.configPath, "config", "", "Config file")
	c.PersistentFlags().StringVar(&opt.database, "database", "", "Database name")
	c.PersistentFlags().StringVar(&opt.rp, "rp", "", "Retention policy name")
	c.PersistentFlags().StringVar(&opt.format, "format", "line", "Output format (line, binary)")
	c.PersistentFlags().StringVar(&opt.conflictPath, "conflict-path", "", "File name for writing field conflicts using line protocol and gzipped")
	c.PersistentFlags().BoolVar(&opt.ignore, "no-conflict-path", false, "Disable writing field conflicts to a file")
	c.PersistentFlags().Var(&opt.r, "range", "Range of target shards to export (default: all)")
	c.PersistentFlags().BoolVar(&opt.print, "print-only", false, "Print plan to stderr and exit")
	c.PersistentFlags().DurationVar(&opt.shardDuration, "shard-duration", time.Hour*24*7, "Target shard duration")

	return c
}

// NewOption returns a new instance of the export Options.
func NewOption(server server.Interface) *Options {
	return &Options{
		Stderr: os.Stderr,
		Stdout: os.Stdout,
		server: server,
	}
}

func (cmd *Options) openExporter() (*exporter, error) {
	cfg := &exporterConfig{Database: cmd.database, RP: cmd.rp, ShardDuration: cmd.shardDuration, Min: cmd.r.Min(), Max: cmd.r.Max()}
	e, err := newExporter(cmd.server, cfg)
	if err != nil {
		return nil, err
	}

	return e, e.Open()
}

type rangeValue struct {
	min, max uint64
	set      bool
}

func (rv *rangeValue) Type() string {
	return "rangeValue"
}

func (rv *rangeValue) Min() uint64 { return rv.min }

func (rv *rangeValue) Max() uint64 {
	if !rv.set {
		return math.MaxUint64
	}
	return rv.max
}

func (rv *rangeValue) String() string {
	if rv.Min() == rv.Max() {
		return fmt.Sprint(rv.min)
	}
	return fmt.Sprintf("[%d,%d]", rv.Min(), rv.Max())
}

func (rv *rangeValue) Set(v string) (err error) {
	p := strings.Split(v, "-")
	switch {
	case len(p) == 1:
		rv.min, err = strconv.ParseUint(p[0], 10, 64)
		if err != nil {
			return fmt.Errorf("range error: invalid number %s", v)
		}
		rv.max = rv.min
	case len(p) == 2:
		rv.min, err = strconv.ParseUint(p[0], 10, 64)
		if err != nil {
			return fmt.Errorf("range error: min value %q is not a positive number", p[0])
		}
		rv.max = math.MaxUint64
		if len(p[1]) > 0 {
			rv.max, err = strconv.ParseUint(p[1], 10, 64)
			if err != nil {
				return fmt.Errorf("range error: max value %q is not empty or a positive number", p[1])
			}
		}
	default:
		return fmt.Errorf("range error: %q is not a valid range", v)
	}

	if rv.min > rv.max {
		return errors.New("range error: min > max")
	}

	rv.set = true

	return nil
}
