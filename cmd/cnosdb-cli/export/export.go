package export

import (
	"bufio"
	"compress/gzip"
	"fmt"
	"io"
	"math"
	"os"
	"os/user"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cnosdatabase/cnosdb/pkg/escape"
	"github.com/cnosdatabase/cnosql"
	"github.com/cnosdatabase/db/models"
	"github.com/cnosdatabase/db/tsdb/engine/tsm1"
	"github.com/spf13/cobra"
)

const examples = `aaa
`

var (
	start     string
	end       string
	exportCmd = NewCommand()
)

func GetCommand() *cobra.Command {
	c := &cobra.Command{
		Use:     "export",
		Short:   "exports raw data from a shard to line protocol",
		Long:    "Exports TSM files into CnosDB line protocol format.",
		Example: examples,
		CompletionOptions: cobra.CompletionOptions{
			DisableDefaultCmd:   true,
			DisableDescriptions: true,
			DisableNoDescFlag:   true,
		},
		PreRun: func(cmd *cobra.Command, args []string) {
			if start != "" {
				s, err := time.Parse(time.RFC3339, start)
				if err != nil {
					fmt.Printf("[ERR] %s\n", err)
					return
				}
				exportCmd.startTime = s.UnixNano()
			} else {
				exportCmd.startTime = math.MinInt64
			}
			if end != "" {
				e, err := time.Parse(time.RFC3339, end)
				if err != nil {
					fmt.Printf("[ERR] %s\n", err)
					return
				}
				exportCmd.endTime = e.UnixNano()
			} else {
				// set end time to max if it is not set.
				exportCmd.endTime = math.MaxInt64
			}

			if err := exportCmd.validate(); err != nil {
				fmt.Printf("[ERR] %s\n", err)
				return
			}
		},
		Run: func(cmd *cobra.Command, args []string) {
			if err := exportCmd.export(); err != nil {
				fmt.Printf("[ERR] %s\n", err)
				return
			}
		},
	}

	var homeDir string
	// By default, store meta and data files in current users home directory
	u, err := user.Current()
	if err == nil {
		homeDir = u.HomeDir
	} else if os.Getenv("HOME") != "" {
		homeDir = os.Getenv("HOME")
	} else {
		fmt.Println("failed to determine current user for storage")
	}

	fs := c.Flags()
	fs.StringVar(&exportCmd.dataDir, "datadir", homeDir+"/.cnosdb/data", "Data storage path")
	fs.StringVar(&exportCmd.walDir, "waldir", homeDir+"/.cnosdb/wal", "WAL storage path")
	fs.StringVar(&exportCmd.out, "out", os.Getenv("HOME")+"/.cnosdb/export", "Destination file to export to")
	fs.StringVar(&exportCmd.database, "database", "", "Optional: the database to export")
	fs.StringVar(&exportCmd.timeToLive, "ttl", "", "Optional: the time to live to export (requires -database)")
	fs.StringVar(&start, "start", "", "Optional: the start time to export (RFC3339 format)")
	fs.StringVar(&end, "end", "", "Optional: the end time to export (RFC3339 format)")
	fs.BoolVar(&exportCmd.compress, "compress", false, "Compress the output")

	return c
}

// Command represents the program execution for "cnosdb-cli export".
type Command struct {
	// Standard input/output, overridden for testing.
	Stderr io.Writer
	Stdout io.Writer

	dataDir    string
	walDir     string
	out        string
	database   string
	timeToLive string
	startTime  int64
	endTime    int64
	compress   bool

	manifest map[string]struct{}
	tsmFiles map[string][]string
	walFiles map[string][]string
}

// NewCommand returns a new instance of Command.
func NewCommand() *Command {
	return &Command{
		Stderr: os.Stderr,
		Stdout: os.Stdout,

		manifest: make(map[string]struct{}),
		tsmFiles: make(map[string][]string),
		walFiles: make(map[string][]string),
	}
}

func (cmd *Command) validate() error {
	if cmd.timeToLive != "" && cmd.database == "" {
		return fmt.Errorf("must specify a db")
	}
	if cmd.startTime != 0 && cmd.endTime != 0 && cmd.endTime < cmd.startTime {
		return fmt.Errorf("end time before start time")
	}
	return nil
}

func (cmd *Command) export() error {
	if err := cmd.walkTSMFiles(); err != nil {
		return err
	}
	if err := cmd.walkWALFiles(); err != nil {
		return err
	}
	return cmd.write()
}

func (cmd *Command) walkTSMFiles() error {
	return filepath.Walk(cmd.dataDir, func(path string, f os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// check to see if this is a tsm file
		if filepath.Ext(path) != "."+tsm1.TSMFileExtension {
			return nil
		}

		relPath, err := filepath.Rel(cmd.dataDir, path)
		if err != nil {
			return err
		}
		dirs := strings.Split(relPath, string(byte(os.PathSeparator)))
		if len(dirs) < 2 {
			return fmt.Errorf("invalid directory structure for %s", path)
		}
		if dirs[0] == cmd.database || cmd.database == "" {
			if dirs[1] == cmd.timeToLive || cmd.timeToLive == "" {
				key := filepath.Join(dirs[0], dirs[1])
				cmd.manifest[key] = struct{}{}
				cmd.tsmFiles[key] = append(cmd.tsmFiles[key], path)
			}
		}
		return nil
	})
}

func (cmd *Command) walkWALFiles() error {
	return filepath.Walk(cmd.walDir, func(path string, f os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// check to see if this is a wal file
		fileName := filepath.Base(path)
		if filepath.Ext(path) != "."+tsm1.WALFileExtension || !strings.HasPrefix(fileName, tsm1.WALFilePrefix) {
			return nil
		}

		relPath, err := filepath.Rel(cmd.walDir, path)
		if err != nil {
			return err
		}
		dirs := strings.Split(relPath, string(byte(os.PathSeparator)))
		if len(dirs) < 2 {
			return fmt.Errorf("invalid directory structure for %s", path)
		}
		if dirs[0] == cmd.database || cmd.database == "" {
			if dirs[1] == cmd.timeToLive || cmd.timeToLive == "" {
				key := filepath.Join(dirs[0], dirs[1])
				cmd.manifest[key] = struct{}{}
				cmd.walFiles[key] = append(cmd.walFiles[key], path)
			}
		}
		return nil
	})
}

func (cmd *Command) write() error {
	// open our output file and create an output buffer
	f, err := os.Create(cmd.out)
	if err != nil {
		return err
	}
	defer f.Close()

	// Because calling (*os.File).Write is relatively expensive,
	// and we don't *need* to sync to disk on every written line of export,
	// use a sized buffered writer so that we only sync the file every megabyte.
	bw := bufio.NewWriterSize(f, 1024*1024)
	defer bw.Flush()

	var w io.Writer = bw

	if cmd.compress {
		gzw := gzip.NewWriter(w)
		defer gzw.Close()
		w = gzw
	}

	s, e := time.Unix(0, cmd.startTime).Format(time.RFC3339), time.Unix(0, cmd.endTime).Format(time.RFC3339)
	fmt.Fprintf(w, "# CNOSDB EXPORT: %s - %s\n", s, e)

	// Write out all the DDL
	fmt.Fprintln(w, "# DDL")
	for key := range cmd.manifest {
		keys := strings.Split(key, string(os.PathSeparator))
		db, ttl := cnosql.QuoteIdent(keys[0]), cnosql.QuoteIdent(keys[1])
		fmt.Fprintf(w, "CREATE DATABASE %s WITH NAME %s\n", db, ttl)
	}

	fmt.Fprintln(w, "# DML")
	for key := range cmd.manifest {
		keys := strings.Split(key, string(os.PathSeparator))
		fmt.Fprintf(w, "# CONTEXT-DATABASE:%s\n", keys[0])
		fmt.Fprintf(w, "# CONTEXT-TTL:%s\n", keys[1])
		if files, ok := cmd.tsmFiles[key]; ok {
			fmt.Fprintf(cmd.Stdout, "writing out tsm file data for %s...", key)
			if err := cmd.writeTsmFiles(w, files); err != nil {
				return err
			}
			fmt.Fprintln(cmd.Stdout, "complete.")
		}
		if _, ok := cmd.walFiles[key]; ok {
			fmt.Fprintf(cmd.Stdout, "writing out wal file data for %s...", key)
			if err := cmd.writeWALFiles(w, cmd.walFiles[key], key); err != nil {
				return err
			}
			fmt.Fprintln(cmd.Stdout, "complete.")
		}
	}
	return nil
}

func (cmd *Command) writeTsmFiles(w io.Writer, files []string) error {
	fmt.Fprintln(w, "# writing tsm data")

	// we need to make sure we write the same order that the files were written
	sort.Strings(files)

	for _, f := range files {
		if err := cmd.exportTSMFile(f, w); err != nil {
			return err
		}
	}

	return nil
}

func (cmd *Command) exportTSMFile(tsmFilePath string, w io.Writer) error {
	f, err := os.Open(tsmFilePath)
	if err != nil {
		if os.IsNotExist(err) {
			fmt.Fprintf(w, "skipped missing file: %s", tsmFilePath)
			return nil
		}
		return err
	}
	defer f.Close()

	r, err := tsm1.NewTSMReader(f)
	if err != nil {
		fmt.Fprintf(cmd.Stderr, "unable to read %s, skipping: %s\n", tsmFilePath, err.Error())
		return nil
	}
	defer r.Close()

	if sgStart, sgEnd := r.TimeRange(); sgStart > cmd.endTime || sgEnd < cmd.startTime {
		return nil
	}

	for i := 0; i < r.KeyCount(); i++ {
		key, _ := r.KeyAt(i)
		values, err := r.ReadAll(key)
		if err != nil {
			fmt.Fprintf(cmd.Stderr, "unable to read key %q in %s, skipping: %s\n", string(key), tsmFilePath, err.Error())
			continue
		}
		metric, field := tsm1.SeriesAndFieldFromCompositeKey(key)
		field = escape.Bytes(field)

		if err := cmd.writeValues(w, metric, string(field), values); err != nil {
			// An error from writeValues indicates an IO error, which should be returned.
			return err
		}
	}
	return nil
}

func (cmd *Command) writeWALFiles(w io.Writer, files []string, key string) error {
	fmt.Fprintln(w, "# writing wal data")

	// we need to make sure we write the same order that the wal received the data
	sort.Strings(files)

	var once sync.Once
	warnDelete := func() {
		once.Do(func() {
			msg := fmt.Sprintf(`WARNING: detected deletes in wal file.
Some series for %q may be brought back by replaying this data.
To resolve, you can either let the shard snapshot prior to exporting the data
or manually editing the exported file.
			`, key)
			fmt.Fprintln(cmd.Stderr, msg)
		})
	}

	for _, f := range files {
		if err := cmd.exportWALFile(f, w, warnDelete); err != nil {
			return err
		}
	}

	return nil
}

// exportWAL reads every WAL entry from r and exports it to w.
func (cmd *Command) exportWALFile(walFilePath string, w io.Writer, warnDelete func()) error {
	f, err := os.Open(walFilePath)
	if err != nil {
		if os.IsNotExist(err) {
			fmt.Fprintf(w, "skipped missing file: %s", walFilePath)
			return nil
		}
		return err
	}
	defer f.Close()

	r := tsm1.NewWALSegmentReader(f)
	defer r.Close()

	for r.Next() {
		entry, err := r.Read()
		if err != nil {
			n := r.Count()
			fmt.Fprintf(cmd.Stderr, "file %s corrupt at position %d: %v", walFilePath, n, err)
			break
		}

		switch t := entry.(type) {
		case *tsm1.DeleteWALEntry, *tsm1.DeleteRangeWALEntry:
			warnDelete()
			continue
		case *tsm1.WriteWALEntry:
			for key, values := range t.Values {
				metric, field := tsm1.SeriesAndFieldFromCompositeKey([]byte(key))
				// metrics are stored escaped, field names are not
				field = escape.Bytes(field)

				if err := cmd.writeValues(w, metric, string(field), values); err != nil {
					// An error from writeValues indicates an IO error, which should be returned.
					return err
				}
			}
		}
	}
	return nil
}

// writeValues writes every value in values to w, using the given series key and field name.
// If any call to w.Write fails, that error is returned.
func (cmd *Command) writeValues(w io.Writer, seriesKey []byte, field string, values []tsm1.Value) error {
	buf := []byte(string(seriesKey) + " " + field + "=")
	prefixLen := len(buf)

	for _, value := range values {
		ts := value.UnixNano()
		if (ts < cmd.startTime) || (ts > cmd.endTime) {
			continue
		}

		// Re-slice buf to be "<series_key> <field>=".
		buf = buf[:prefixLen]

		// Append the correct representation of the value.
		switch v := value.Value().(type) {
		case float64:
			buf = strconv.AppendFloat(buf, v, 'g', -1, 64)
		case int64:
			buf = strconv.AppendInt(buf, v, 10)
			buf = append(buf, 'i')
		case uint64:
			buf = strconv.AppendUint(buf, v, 10)
			buf = append(buf, 'u')
		case bool:
			buf = strconv.AppendBool(buf, v)
		case string:
			buf = append(buf, '"')
			buf = append(buf, models.EscapeStringField(v)...)
			buf = append(buf, '"')
		default:
			// This shouldn't be possible, but we'll format it anyway.
			buf = append(buf, fmt.Sprintf("%v", v)...)
		}

		// Now buf has "<series_key> <field>=<value>".
		// Append the timestamp and a newline, then write it.
		buf = append(buf, ' ')
		buf = strconv.AppendInt(buf, ts, 10)
		buf = append(buf, '\n')
		if _, err := w.Write(buf); err != nil {
			// Underlying IO error needs to be returned.
			return err
		}
	}

	return nil
}
