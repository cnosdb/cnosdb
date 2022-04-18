package compact

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"time"

	"github.com/cnosdb/cnosdb/cmd/cnosdb_tools/internal/errlist"
	"github.com/cnosdb/cnosdb/cmd/cnosdb_tools/internal/format/binary"
	"github.com/cnosdb/cnosdb/cmd/cnosdb_tools/internal/format/line"
	"github.com/cnosdb/cnosdb/vend/db/logger"
	"github.com/cnosdb/cnosdb/vend/db/pkg/limiter"
	"github.com/cnosdb/cnosdb/vend/db/tsdb/engine/tsm1"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

var (
	_ line.Writer
	_ binary.Writer
)

// Options represents the program execution for "cnosdb-tools compact".
type Options struct {
	// Standard input/output, overridden for testing.
	Stderr io.Writer
	Stdout io.Writer
	Logger *zap.Logger

	path    string
	force   bool
	verbose bool
}

// NewOptions returns a new instance of the export Command.
func NewOptions() *Options {
	return &Options{
		Stderr: os.Stderr,
		Stdout: os.Stdout,
	}
}

var opt = NewOptions()

func GetCommand() *cobra.Command {
	c := &cobra.Command{
		Use:   "compact",
		Short: "fully compacts the specified shard.",
		RunE: func(cmd *cobra.Command, args []string) error {
			var err error
			var log = zap.NewNop()
			opt.path = args[0]
			if opt.path == "" {
				return errors.New("shard-path is required")
			}
			if opt.verbose {
				cfg := logger.Config{Format: "logfmt"}
				log, err = cfg.New(os.Stdout)
				if err != nil {
					return err
				}
			}

			fmt.Fprintf(opt.Stdout, "opening shard at path %q\n\n", opt.path)

			sc, err := newShardCompactor(opt.path, log)
			if err != nil {
				return err
			}

			fmt.Fprintln(opt.Stdout)
			fmt.Fprintln(opt.Stdout, "The following files will be compacted:")
			fmt.Fprintln(opt.Stdout)
			fmt.Fprintln(opt.Stdout, sc.String())

			if !opt.force {
				fmt.Fprint(opt.Stdout, "Proceed? [N] ")
				scan := bufio.NewScanner(os.Stdin)
				scan.Scan()
				if scan.Err() != nil {
					return fmt.Errorf("error reading STDIN: %v", scan.Err())
				}

				if strings.ToLower(scan.Text()) != "y" {
					return nil
				}
			}

			fmt.Fprintln(opt.Stdout, "Compacting shard.")

			err = sc.CompactShard()
			if err != nil {
				return fmt.Errorf("compaction failed: %v", err)
			}

			fmt.Fprintln(opt.Stdout, "Compaction succeeded. New files:")
			for _, f := range sc.newTSM {
				fmt.Fprintf(opt.Stdout, "  %s\n", f)
			}

			return nil
		},
	}
	c.PersistentFlags().StringVar(&opt.path, "path", "", "Path to shard to be compacted.")
	c.PersistentFlags().BoolVar(&opt.force, "force", false, "force compaction without prompting")
	c.PersistentFlags().BoolVar(&opt.verbose, "verbose", false, "Enable verbose logging")
	return c
}

type shardCompactor struct {
	logger    *zap.Logger
	path      string
	tsm       []string
	tombstone []string
	readers   []*tsm1.TSMReader
	files     map[string]*tsm1.TSMReader
	newTSM    []string
}

func newShardCompactor(path string, logger *zap.Logger) (sc *shardCompactor, err error) {
	sc = &shardCompactor{
		logger: logger,
		path:   path,
		files:  make(map[string]*tsm1.TSMReader),
	}

	sc.tsm, err = filepath.Glob(filepath.Join(path, fmt.Sprintf("*.%s", tsm1.TSMFileExtension)))
	if err != nil {
		return nil, fmt.Errorf("newFileStore: error reading tsm files at path %q: %v", path, err)
	}
	if len(sc.tsm) == 0 {
		return nil, fmt.Errorf("newFileStore: no tsm files at path %q", path)
	}
	sort.Strings(sc.tsm)

	sc.tombstone, err = filepath.Glob(filepath.Join(path, fmt.Sprintf("*.%s", "tombstone")))
	if err != nil {
		return nil, fmt.Errorf("error reading tombstone files: %v", err)
	}

	if err := sc.openFiles(); err != nil {
		return nil, err
	}

	return sc, nil
}

func (sc *shardCompactor) openFiles() error {
	sc.readers = make([]*tsm1.TSMReader, 0, len(sc.tsm))

	// struct to hold the result of opening each reader in a goroutine
	type res struct {
		r   *tsm1.TSMReader
		err error
	}

	lim := limiter.NewFixed(runtime.GOMAXPROCS(0))

	readerC := make(chan *res)
	for i, fn := range sc.tsm {
		file, err := os.OpenFile(fn, os.O_RDONLY, 0666)
		if err != nil {
			return fmt.Errorf("newFileStore: failed to open file %q: %v", fn, err)
		}

		go func(idx int, file *os.File) {
			// Ensure a limited number of TSM files are loaded at once.
			// Systems which have very large datasets (1TB+) can have thousands
			// of TSM files which can cause extremely long load times.
			lim.Take()
			defer lim.Release()

			start := time.Now()
			df, err := tsm1.NewTSMReader(file)
			sc.logger.Info("Opened file",
				zap.String("path", file.Name()),
				zap.Int("id", idx),
				zap.Duration("duration", time.Since(start)))

			// If we are unable to read a TSM file then log the error, rename
			// the file, and continue loading the shard without it.
			if err != nil {
				sc.logger.Error("Cannot read corrupt tsm file, renaming", zap.String("path", file.Name()), zap.Int("id", idx), zap.Error(err))
				if e := os.Rename(file.Name(), file.Name()+"."+tsm1.BadTSMFileExtension); e != nil {
					sc.logger.Error("Cannot rename corrupt tsm file", zap.String("path", file.Name()), zap.Int("id", idx), zap.Error(e))
					readerC <- &res{r: df, err: fmt.Errorf("cannot rename corrupt file %s: %v", file.Name(), e)}
					return
				}
			}

			readerC <- &res{r: df}
		}(i, file)
	}

	for range sc.tsm {
		res := <-readerC
		if res.err != nil {
			return res.err
		} else if res.r == nil {
			continue
		}
		sc.readers = append(sc.readers, res.r)
		sc.files[res.r.Path()] = res.r
	}
	close(readerC)
	sort.Sort(tsmReaders(sc.readers))

	return nil
}

func (sc *shardCompactor) CompactShard() (err error) {
	c := tsm1.NewCompactor()
	c.Dir = sc.path
	c.Size = tsm1.DefaultSegmentSize
	c.FileStore = sc
	c.Open()

	tsmFiles, err := c.CompactFull(sc.tsm)
	if err == nil {
		sc.newTSM, err = sc.replace(tsmFiles)
	}
	return err
}

// replace replaces the existing shard files with temporary tsmFiles
func (sc *shardCompactor) replace(tsmFiles []string) ([]string, error) {
	// rename .tsm.tmp → .tsm
	var newNames []string
	for _, file := range tsmFiles {
		var newName = file[:len(file)-4] // remove extension
		if err := os.Rename(file, newName); err != nil {
			return nil, err
		}
		newNames = append(newNames, newName)
	}

	var errs errlist.ErrorList

	// close all readers
	for _, r := range sc.readers {
		r.Close()
	}

	sc.readers = nil
	sc.files = nil

	// remove existing .tsm and .tombstone
	for _, file := range sc.tsm {
		errs.Add(os.Remove(file))
	}

	for _, file := range sc.tombstone {
		errs.Add(os.Remove(file))
	}

	return newNames, errs.Err()
}

func (sc *shardCompactor) NextGeneration() int {
	panic("not implemented")
}

func (sc *shardCompactor) TSMReader(path string) *tsm1.TSMReader {
	r := sc.files[path]
	if r != nil {
		r.Ref()
	}
	return r
}

func (sc *shardCompactor) String() string {
	var sb bytes.Buffer
	sb.WriteString("TSM:\n")
	for _, f := range sc.tsm {
		sb.WriteString("  ")
		sb.WriteString(f)
		sb.WriteByte('\n')
	}

	if len(sc.tombstone) > 0 {
		sb.WriteString("\nTombstone:\n")
		for _, f := range sc.tombstone {
			sb.WriteString("  ")
			sb.WriteString(f)
			sb.WriteByte('\n')
		}
	}

	return sb.String()
}

type tsmReaders []*tsm1.TSMReader

func (a tsmReaders) Len() int           { return len(a) }
func (a tsmReaders) Less(i, j int) bool { return a[i].Path() < a[j].Path() }
func (a tsmReaders) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
