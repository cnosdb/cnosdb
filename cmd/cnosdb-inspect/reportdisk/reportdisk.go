package reportdisk

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/user"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"time"

	"github.com/cnosdb/cnosdb/vend/db/models"
	"github.com/cnosdb/cnosdb/vend/db/pkg/reporthelper"
	"github.com/cnosdb/cnosdb/vend/db/tsdb/engine/tsm1"

	"github.com/spf13/cobra"
)

type Options struct {
	Stderr io.Writer
	Stdout io.Writer

	dir      string
	detailed bool
}

func NewOptions() *Options {
	return &Options{
		Stderr: os.Stderr,
		Stdout: os.Stdout,
	}
}

var opt = NewOptions()

func GetCommand() *cobra.Command {
	c := &cobra.Command{
		Use:   "report-disk [path]",
		Short: "Review disk usage by shards and measurements for TSM files in a specified directory.",
		Long:  "Use the report-disk command to review disk usage by shards and measurements for TSM files in a specified directory. Useful for determining disk usage for capacity planning and identifying which measurements or shards are using the most space.",
		Args:  cobra.MaximumNArgs(1),

		Run: func(cmd *cobra.Command, args []string) {
			if len(args) > 0 {
				opt.dir = args[0]
			} else {
				u, err := user.Current()
				if err == nil {
					opt.dir = path.Join(u.HomeDir, ".cnosdb/data")
				} else if os.Getenv("HOME") != "" {
					opt.dir = path.Join(os.Getenv("HOME"), ".cnosdb/data")
				} else {
					panic(errors.New("can not get default path"))
				}
			}
			if err := reportDisk(); err != nil {
				panic(err)
			}
		},
	}

	//c.PersistentFlags().StringVar(&opt.dir, "path", defaultDir, "Path to the directory with .tsm file(s) to report disk usage for. Default location is \"$HOME/.cnosdb/data\".\nWhen specifying the path, wildcards (*) can replace one or more characters.")
	c.PersistentFlags().BoolVar(&opt.detailed, "detailed", false, "Include this flag to report disk usage by measurement.")
	return c
}
func sanitize(s string) []byte {
	b, _ := json.Marshal(s) // json shouldn't be throwing errors when marshalling a string
	return b
}
func reportDisk() error {
	start := time.Now()

	dirs, _ := filepath.Glob(opt.dir)
	if dirs == nil {
		fmt.Fprintf(opt.Stderr, "error: there are no directories matching '%s'.\n", opt.dir)
		return nil
	}
	shardSizes := ShardSizes{}
	for _, subDir := range dirs {
		if err := reporthelper.WalkShardDirs(subDir, func(db, rp, id, path string) error {

			stat, err := os.Stat(path)
			if err != nil {
				fmt.Fprintf(opt.Stderr, "error: %s: %v. Skipping.\n", path, err)
				return nil
			}

			shardSizes.AddTsmFileWithSize(db, rp, id, stat.Size())
			return nil
		}); err != nil {
			return err
		}
	}

	measurementSizes := MeasurementSizes{}
	if opt.detailed {
		processedFiles := 0
		progress := NewProgressReporter(opt.Stderr)
		for _, subDir := range dirs {
			if err := reporthelper.WalkShardDirs(subDir,
				func(db, rp, id, path string) error {
					file, err := os.OpenFile(path, os.O_RDONLY, 0600)
					if err != nil {
						fmt.Fprintf(opt.Stderr, "error: %s: %v. Skipping.\n", path, err)
						return nil
					}

					progress.Report(int64(processedFiles), shardSizes.files)
					processedFiles++

					reader, err := tsm1.NewTSMReader(file)
					if err != nil {
						fmt.Fprintf(opt.Stderr, "error: %s: %v. Skipping.\n", file.Name(), err)
						return nil
					}

					keyNum := reader.KeyCount()
					for i := 0; i < keyNum; i++ {
						key, _ := reader.KeyAt(i)
						series, _ := tsm1.SeriesAndFieldFromCompositeKey(key)
						measurement := models.ParseName(series)
						var size int64
						for _, entry := range reader.Entries(key) {
							size += int64(entry.Size)
						}
						measurementSizes.AddSize(db, rp, string(measurement), size)
					}
					return nil
				}); err != nil {
				return err
			}
		}
		progress.Report(int64(processedFiles), shardSizes.files)
	}
	fmt.Fprintf(opt.Stderr, "\nCompleted in %s\n", time.Since(start))

	fmt.Fprintf(opt.Stdout, "{\n  \"Summary\": {\"shards\": %d, \"tsm_files\": %d, \"total_tsm_size\": %d },\n  \"Shard\": [", shardSizes.shards, shardSizes.files, shardSizes.totalSize)

	first := true
	shardSizes.ForEach(func(db, rp, id string, detail ShardDetails) {
		var shardString []byte
		if s, err := strconv.ParseInt(id, 10, 64); err != nil && strconv.FormatInt(s, 10) == id {
			shardString = []byte(id)
		} else {
			shardString = sanitize(id)
		}
		if !first {
			fmt.Fprint(opt.Stdout, ",")
		}
		first = false
		fmt.Fprintf(opt.Stdout, "\n   {\"db\": %s, \"rp\": %s, \"shard\": %s, \"tsm_files\": %d, \"size\": %d}",
			sanitize(db), sanitize(rp), shardString, detail.files, detail.size)
	})

	if opt.detailed {
		fmt.Fprintf(opt.Stdout, "\n  ],\n  \"Measurement\": [")

		first = true
		measurementSizes.ForEach(func(db, rp, measurement string, size int64) {
			if !first {
				fmt.Fprint(opt.Stdout, ",")
			}
			first = false
			fmt.Fprintf(opt.Stdout, "\n   {\"db\": %s, \"rp\": %s, \"measurement\": %s, \"size\": %d}",
				sanitize(db), sanitize(rp), sanitize(measurement), size)
		})

	}
	fmt.Fprintf(opt.Stdout, "\n  ]\n}\n")
	return nil
}

type ShardDetails struct {
	size  int64
	files int64
}

type ShardSizes struct {
	m         map[string]map[string]map[string]*ShardDetails
	files     int64
	shards    int64
	totalSize int64
}

func (s *ShardSizes) AddTsmFileWithSize(db, rp, id string, size int64) {
	if s.m == nil {
		s.m = make(map[string]map[string]map[string]*ShardDetails)
	}
	if _, ok := s.m[db]; !ok {
		s.m[db] = make(map[string]map[string]*ShardDetails)
	}
	if _, ok := s.m[db][rp]; !ok {
		s.m[db][rp] = make(map[string]*ShardDetails)
	}
	if _, ok := s.m[db][rp][id]; !ok {
		s.m[db][rp][id] = &ShardDetails{}
		s.shards += 1
	}
	s.m[db][rp][id].size += size
	s.m[db][rp][id].files += 1
	s.files += 1
	s.totalSize += size
}

func (s *ShardSizes) ForEach(f func(db, rp, id string, detail ShardDetails)) {
	dbKeys := make([]string, 0, len(s.m))
	for db, _ := range s.m {
		dbKeys = append(dbKeys, db)
	}
	sort.Strings(dbKeys)
	for _, db := range dbKeys {
		rpKeys := make([]string, 0, len(s.m[db]))
		for rp, _ := range s.m[db] {
			rpKeys = append(rpKeys, rp)
		}
		sort.Strings(rpKeys)
		for _, rp := range rpKeys {
			idKeys := make([]string, 0, len(s.m[db][rp]))
			for id, _ := range s.m[db][rp] {
				idKeys = append(idKeys, id)
			}
			sort.Strings(idKeys)
			for _, id := range idKeys {
				f(db, rp, id, *s.m[db][rp][id])
			}
		}
	}
}

type MeasurementSizes struct {
	m map[string]map[string]map[string]int64
}

func (s *MeasurementSizes) AddSize(db, rp, measurement string, size int64) {
	if s.m == nil {
		s.m = make(map[string]map[string]map[string]int64)
	}
	if _, ok := s.m[db]; !ok {
		s.m[db] = make(map[string]map[string]int64)
	}
	if _, ok := s.m[db][rp]; !ok {
		s.m[db][rp] = make(map[string]int64)
	}
	if _, ok := s.m[db][rp][measurement]; !ok {
		s.m[db][rp][measurement] = 0
	}
	s.m[db][rp][measurement] += size
}

func (s *MeasurementSizes) ForEach(f func(db, rp, measurement string, size int64)) {
	dbKeys := make([]string, 0, len(s.m))
	for db, _ := range s.m {
		dbKeys = append(dbKeys, db)
	}
	sort.Strings(dbKeys)
	for _, db := range dbKeys {
		rpKeys := make([]string, 0, len(s.m[db]))
		for rp, _ := range s.m[db] {
			rpKeys = append(rpKeys, rp)
		}
		sort.Strings(rpKeys)
		for _, rp := range rpKeys {
			mKeys := make([]string, 0, len(s.m[db][rp]))
			for m, _ := range s.m[db][rp] {
				mKeys = append(mKeys, m)
			}
			sort.Strings(mKeys)
			for _, m := range mKeys {
				f(db, rp, m, s.m[db][rp][m])
			}
		}
	}
}

type ProgressReporter struct {
	lastLength int
	w          io.Writer
}

func NewProgressReporter(w io.Writer) *ProgressReporter {
	return &ProgressReporter{w: w}
}

func (p *ProgressReporter) Report(current int64, total int64) {
	line := fmt.Sprintf("TSM files inspected: %d\t/%d", current, total)
	if p.lastLength == 0 {
		fmt.Fprintf(p.w, "\n")
		p.lastLength = 1
	}
	for len(line) < p.lastLength {
		line += " "
	}
	p.lastLength = len(line)
	fmt.Fprint(p.w, "\r"+line)
}
