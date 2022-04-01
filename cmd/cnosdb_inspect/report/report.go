// Package report reports statistics about TSM files.
package report

import (
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/cnosdb/cnosdb/vend/db/models"
	"github.com/cnosdb/cnosdb/vend/db/pkg/reporthelper"
	"github.com/cnosdb/cnosdb/vend/db/tsdb/engine/tsm1"

	"github.com/retailnext/hllpp"
	"github.com/spf13/cobra"
)

var opt Option

type Option struct {
	dir             string
	pattern         string
	detailed, exact bool

	test string
}

func GetCommand() *cobra.Command {
	c := &cobra.Command{
		Use:   "report",
		Short: "Displays series metadata for all shards",
		Run: func(cmd *cobra.Command, args []string) {

			if err := report(cmd, args); err != nil {
				cmd.PrintErrln(err)
			}

		},
	}
	c.SetUsageFunc(func(cmd *cobra.Command) error {
		cmd.Print(usage)
		return nil
	})
	c.PersistentFlags().StringVar(&opt.pattern, "pattern", "", "Include only files matching a pattern")
	c.PersistentFlags().BoolVar(&opt.detailed, "detailed", false, "Report detailed cardinality estimates")
	c.PersistentFlags().BoolVar(&opt.exact, "exact", false, "Report exact counts")
	return c
}

func report(cmd *cobra.Command, args []string) error {
	if err := cmd.ParseFlags(args); err != nil {
		cmd.PrintErr(err)
	}
	newCounterFn := newHLLCounter
	estTitle := " (est)"
	if opt.exact {
		estTitle = ""
		newCounterFn = newExactCounter
	}

	if len(args) == 0 {
		return fmt.Errorf("shard dirs must be specified")
	}
	opt.dir = args[0]

	err := reporthelper.IsShardDir(opt.dir)
	if opt.detailed && err != nil {
		return fmt.Errorf("--detailed only supported for shard dirs")
	}

	totalSeries := newCounterFn()
	tagCardinalities := map[string]counter{}
	measCardinalities := map[string]counter{}
	fieldCardinalities := map[string]counter{}

	dbCardinalities := map[string]counter{}

	start := time.Now()

	tw := tabwriter.NewWriter(cmd.OutOrStdout(), 8, 2, 1, ' ', 0)
	fmt.Fprintln(tw, strings.Join([]string{"DB", "RP", "Shard", "File", "Series", "New" + estTitle, "Min Time", "Max Time", "Load Time"}, "\t"))

	minTime, maxTime := int64(math.MaxInt64), int64(math.MinInt64)
	var fileCount int
	if err := reporthelper.WalkShardDirs(opt.dir, func(db, rp, id, path string) error {
		if opt.pattern != "" && !strings.Contains(path, opt.pattern) {
			return nil
		}

		file, err := os.OpenFile(path, os.O_RDONLY, 0600)
		if err != nil {
			fmt.Fprintf(cmd.OutOrStderr(), "error: %s: %v. Skipping.\n", path, err)
			return nil
		}

		loadStart := time.Now()
		reader, err := tsm1.NewTSMReader(file)
		if err != nil {
			fmt.Fprintf(cmd.OutOrStderr(), "error: %s: %v. Skipping.\n", file.Name(), err)
			return nil
		}
		loadTime := time.Since(loadStart)
		fileCount++

		dbCount := dbCardinalities[db]
		if dbCount == nil {
			dbCount = newCounterFn()
			dbCardinalities[db] = dbCount
		}

		oldCount := dbCount.Count()

		seriesCount := reader.KeyCount()
		for i := 0; i < seriesCount; i++ {
			key, _ := reader.KeyAt(i)
			totalSeries.Add(key)
			dbCount.Add(key)

			if opt.detailed {
				sep := strings.Index(string(key), "#!~#")
				seriesKey, field := key[:sep], key[sep+4:]
				measurement, tags := models.ParseKey(seriesKey)

				measCount := measCardinalities[measurement]
				if measCount == nil {
					measCount = newCounterFn()
					measCardinalities[measurement] = measCount
				}
				measCount.Add(key)

				fieldCount := fieldCardinalities[measurement]
				if fieldCount == nil {
					fieldCount = newCounterFn()
					fieldCardinalities[measurement] = fieldCount
				}
				fieldCount.Add(field)

				for _, t := range tags {
					tagCount := tagCardinalities[string(t.Key)]
					if tagCount == nil {
						tagCount = newCounterFn()
						tagCardinalities[string(t.Key)] = tagCount
					}
					tagCount.Add(t.Value)
				}
			}
		}
		minT, maxT := reader.TimeRange()
		if minT < minTime {
			minTime = minT
		}
		if maxT > maxTime {
			maxTime = maxT
		}
		reader.Close()

		fmt.Fprintln(tw, strings.Join([]string{
			db, rp, id,
			filepath.Base(file.Name()),
			strconv.FormatInt(int64(seriesCount), 10),
			strconv.FormatInt(int64(dbCount.Count()-oldCount), 10),
			time.Unix(0, minT).UTC().Format(time.RFC3339Nano),
			time.Unix(0, maxT).UTC().Format(time.RFC3339Nano),
			loadTime.String(),
		}, "\t"))
		if opt.detailed {
			tw.Flush()
		}
		return nil
	}); err != nil {
		return err
	}

	tw.Flush()
	println()

	println("Summary:")
	fmt.Printf("  Files: %d\n", fileCount)
	fmt.Printf("  Time Range: %s - %s\n",
		time.Unix(0, minTime).UTC().Format(time.RFC3339Nano),
		time.Unix(0, maxTime).UTC().Format(time.RFC3339Nano),
	)
	fmt.Printf("  Duration: %s \n", time.Unix(0, maxTime).Sub(time.Unix(0, minTime)))
	println()

	fmt.Printf("Statistics\n")
	fmt.Printf("  Series:\n")
	for db, counts := range dbCardinalities {
		fmt.Printf("     - %s%s: %d (%d%%)\n", db, estTitle, counts.Count(), int(float64(counts.Count())/float64(totalSeries.Count())*100))
	}
	fmt.Printf("  Total%s: %d\n", estTitle, totalSeries.Count())

	if opt.detailed {
		fmt.Printf("\n  Measurements (est):\n")
		for _, t := range sortKeys(measCardinalities) {
			fmt.Printf("    - %v: %d (%d%%)\n", t, measCardinalities[t].Count(), int((float64(measCardinalities[t].Count())/float64(totalSeries.Count()))*100))
		}

		fmt.Printf("\n  Fields (est):\n")
		for _, t := range sortKeys(fieldCardinalities) {
			fmt.Printf("    - %v: %d\n", t, fieldCardinalities[t].Count())
		}

		fmt.Printf("\n  Tags (est):\n")
		for _, t := range sortKeys(tagCardinalities) {
			fmt.Printf("    - %v: %d\n", t, tagCardinalities[t].Count())
		}
	}

	fmt.Printf("Completed in %s\n", time.Since(start))
	return nil
}

// sortKeys is a quick helper to return the sorted set of a map's keys
func sortKeys(vals map[string]counter) (keys []string) {
	for k := range vals {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	return keys
}

// printUsage prints the usage message to STDOUT.
var usage = `Displays shard level report.

Usage: cnosdb_inspect report [flags]

   --pattern <pattern>
           Include only files matching a pattern.
   --exact
           Report exact cardinality counts instead of estimates.  Note: this can use a lot of memory.
           Defaults to "false".
   --detailed
           Report detailed cardinality estimates.
           Defaults to "false".
`

// counter abstracts a a method of counting keys.
type counter interface {
	Add(key []byte)
	Count() uint64
}

// newHLLCounter returns an approximate counter using HyperLogLogs for cardinality estimation.
func newHLLCounter() counter {
	return hllpp.New()
}

// exactCounter returns an exact count for keys using counting all distinct items in a set.
type exactCounter struct {
	m map[string]struct{}
}

func (c *exactCounter) Add(key []byte) {
	c.m[string(key)] = struct{}{}
}

func (c *exactCounter) Count() uint64 {
	return uint64(len(c.m))
}

func newExactCounter() counter {
	return &exactCounter{
		m: make(map[string]struct{}),
	}
}
