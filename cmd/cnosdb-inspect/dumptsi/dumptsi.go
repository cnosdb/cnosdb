package dumptsi

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"text/tabwriter"

	errors2 "github.com/cnosdb/cnosdb/pkg/errors"
	"github.com/cnosdb/cnosdb/pkg/logger"
	"github.com/cnosdb/cnosdb/vend/db/models"
	"github.com/cnosdb/cnosdb/vend/db/tsdb"
	_ "github.com/cnosdb/cnosdb/vend/db/tsdb/engine/tsm1"
	"github.com/cnosdb/cnosdb/vend/db/tsdb/index/tsi1"

	"github.com/spf13/cobra"
)

type Options struct {
	Stderr io.Writer
	Stdout io.Writer

	seriesFilePath string
	paths          []string

	showSeries         bool
	showMeasurements   bool
	showTagKeys        bool
	showTagValues      bool
	showTagValueSeries bool

	measurementFilter *regexp.Regexp
	tagKeyFilter      *regexp.Regexp
	tagValueFilter    *regexp.Regexp
}

func NewOptions() *Options {
	return &Options{
		Stderr: os.Stderr,
		Stdout: os.Stdout,
	}
}

var opt = NewOptions()

func GetCommand() *cobra.Command {
	var measurementFilter, tagKeyFilter, tagValueFilter string
	c := &cobra.Command{
		Use:   "dumptsi",
		Short: "Dumps low-level details about tsi1 files.",
		RunE: func(cmd *cobra.Command, args []string) error {
			opt.paths = args
			if measurementFilter != "" {
				re, err := regexp.Compile(measurementFilter)
				if err != nil {
					return err
				}
				opt.measurementFilter = re
			}
			if tagKeyFilter != "" {
				re, err := regexp.Compile(tagKeyFilter)
				if err != nil {
					return err
				}
				opt.tagKeyFilter = re
			}
			if tagValueFilter != "" {
				re, err := regexp.Compile(tagValueFilter)
				if err != nil {
					return err
				}
				opt.tagValueFilter = re
			}
			if opt.seriesFilePath == "" {
				return errors.New("series file path required")
			}
			if opt.paths == nil {
				return errors.New("at least one path required")
			}
			// Some flags imply other flags.
			if opt.showTagValueSeries {
				opt.showTagValues = true
			}
			if opt.showTagValues {
				opt.showTagKeys = true
			}
			if opt.showTagKeys {
				opt.showMeasurements = true
			}
			return opt.run()
		},
	}
	c.PersistentFlags().StringVar(&opt.seriesFilePath, "series-file", "", "Path to series file")
	c.PersistentFlags().BoolVar(&opt.showSeries, "series", false, "Show raw series data")
	c.PersistentFlags().BoolVar(&opt.showMeasurements, "measurements", false, "Show raw measurement data")
	c.PersistentFlags().BoolVar(&opt.showTagKeys, "tag-keys", false, "Show raw tag key data")
	c.PersistentFlags().BoolVar(&opt.showTagValues, "tag-values", false, "Show raw tag value data")
	c.PersistentFlags().BoolVar(&opt.showTagValueSeries, "tag-value-series", false, "Show raw series data for each value")
	c.PersistentFlags().StringVar(&measurementFilter, "measurement-filter", "", "Regex measurement filter")
	c.PersistentFlags().StringVar(&tagKeyFilter, "tag-key-filter", "", "Regex tag key filter")
	c.PersistentFlags().StringVar(&tagValueFilter, "tag-value-filter", "", "Regex tag value filter")
	c.PersistentFlags().SetOutput(opt.Stdout)
	return c
}
func (opt *Options) run() (rErr error) {
	sfile := tsdb.NewSeriesFile(opt.seriesFilePath)
	sfile.Logger = logger.NewLoggerWithWriter(opt.Stderr)
	if err := sfile.Open(); err != nil {
		return err
	}
	defer sfile.Close()

	// Build a file set from the paths on the command line.
	idx, fs, err := opt.readFileSet(sfile)
	if err != nil {
		return err
	}
	if fs != nil {
		defer errors2.Capture(&rErr, fs.Close)()
		defer fs.Release()
	}
	if idx != nil {
		defer errors2.Capture(&rErr, idx.Close)()
	}

	if opt.showSeries {
		if err := opt.printSeries(sfile); err != nil {
			return err
		}
	}

	// If this is an ad-hoc fileset then process it and close afterward.
	if fs != nil {
		if opt.showSeries || opt.showMeasurements {
			return opt.printMeasurements(sfile, fs)
		}
		return opt.printFileSummaries(fs)
	}

	// Otherwise iterate over each partition in the index.
	for i := 0; i < int(idx.PartitionN); i++ {
		if err := func() error {
			fs, err := idx.PartitionAt(i).RetainFileSet()
			if err != nil {
				return err
			}
			defer fs.Release()

			if opt.showSeries || opt.showMeasurements {
				return opt.printMeasurements(sfile, fs)
			}
			return opt.printFileSummaries(fs)
		}(); err != nil {
			return err
		}
	}
	return nil
}

func (opt *Options) readFileSet(sfile *tsdb.SeriesFile) (*tsi1.Index, *tsi1.FileSet, error) {
	// If only one path exists and it's a directory then open as an index.
	if len(opt.paths) == 1 {
		fi, err := os.Stat(opt.paths[0])
		if err != nil {
			return nil, nil, fmt.Errorf("failed to get FileInfo of %q: %w", opt.paths[0], err)
		} else if fi.IsDir() {
			// Verify directory is an index before opening it.
			if ok, err := tsi1.IsIndexDir(opt.paths[0]); err != nil {
				return nil, nil, err
			} else if !ok {
				return nil, nil, fmt.Errorf("not an index directory: %q", opt.paths[0])
			}

			idx := tsi1.NewIndex(sfile,
				"",
				tsi1.WithPath(opt.paths[0]),
				tsi1.DisableCompactions(),
			)
			if err := idx.Open(); err != nil {
				return nil, nil, fmt.Errorf("failed to open TSI Index at %q: %w", idx.Path(), err)
			}
			return idx, nil, nil
		}
	}

	// Open each file and group into a fileset.
	var files []tsi1.File
	for _, path := range opt.paths {
		switch ext := filepath.Ext(path); ext {
		case tsi1.LogFileExt: //LogFileExt   = ".tsl"
			f := tsi1.NewLogFile(sfile, path)
			if err := f.Open(); err != nil {
				return nil, nil, fmt.Errorf("failed to get TSI logfile at %q: %w", sfile.Path(), err)
			}
			files = append(files, f)

		case tsi1.IndexFileExt: //IndexFileExt = ".tsi"
			f := tsi1.NewIndexFile(sfile)
			f.SetPath(path)
			if err := f.Open(); err != nil {
				return nil, nil, fmt.Errorf("failed to open index file at %q: %w", f.Path(), err)
			}
			files = append(files, f)

		default:
			return nil, nil, fmt.Errorf("unexpected file extension: %s", ext)
		}
	}

	fs := tsi1.NewFileSet1(files)
	fs.Retain()

	return nil, fs, nil
}

func (opt *Options) printSeries(sfile *tsdb.SeriesFile) error {
	if !opt.showSeries {
		return nil
	}

	// Print header.
	tw := tabwriter.NewWriter(opt.Stdout, 8, 8, 1, '\t', 0)
	fmt.Fprintln(tw, "Series\t")
	// Iterate over each series.
	itr := sfile.SeriesIDIterator()
	for {
		e, err := itr.Next()
		if err != nil {
			return fmt.Errorf("failed to get next series ID in %q: %w", sfile.Path(), err)
		} else if e.SeriesID == 0 {
			break
		}
		name, tags := tsdb.ParseSeriesKey(sfile.SeriesKey(e.SeriesID))

		if !opt.matchSeries(name, tags) {
			continue
		}

		deleted := sfile.IsDeleted(e.SeriesID)

		fmt.Fprintf(tw, "%s%s\t%v\n", name, tags.HashKey(), deletedString(deleted))
	}

	// Flush & write footer spacing.
	if err := tw.Flush(); err != nil {
		return fmt.Errorf("failed to flush tabwriter: %w", err)
	}
	fmt.Fprint(opt.Stdout, "\n\n")

	return nil
}

func (opt *Options) printMeasurements(sfile *tsdb.SeriesFile, fs *tsi1.FileSet) error {
	if !opt.showMeasurements {
		return nil
	}
	tw := tabwriter.NewWriter(opt.Stdout, 8, 8, 1, '\t', 0)
	fmt.Fprintln(tw, "Measurement\t")

	// Iterate over each series.
	if itr := fs.MeasurementIterator(); itr != nil {
		for e := itr.Next(); e != nil; e = itr.Next() {
			if opt.measurementFilter != nil && !opt.measurementFilter.Match(e.Name()) {
				continue
			}

			fmt.Fprintf(tw, "%s\t%v\n", e.Name(), deletedString(e.Deleted()))
			if err := tw.Flush(); err != nil {
				return fmt.Errorf("failed to flush tabwriter: %w", err)
			}

			if err := opt.printTagKeys(sfile, fs, e.Name()); err != nil {
				return err
			}
		}
	}

	fmt.Fprint(opt.Stdout, "\n\n")

	return nil
}

func (opt *Options) printTagKeys(sfile *tsdb.SeriesFile, fs *tsi1.FileSet, name []byte) error {
	if !opt.showTagKeys {
		return nil
	}

	// Iterate over each key.
	tw := tabwriter.NewWriter(opt.Stdout, 8, 8, 1, '\t', 0)
	itr := fs.TagKeyIterator(name)
	for e := itr.Next(); e != nil; e = itr.Next() {
		if opt.tagKeyFilter != nil && !opt.tagKeyFilter.Match(e.Key()) {
			continue
		}

		fmt.Fprintf(tw, "    %s\t%v\n", e.Key(), deletedString(e.Deleted()))
		if err := tw.Flush(); err != nil {
			return fmt.Errorf("failed to flush tabwriter: %w", err)
		}

		if err := opt.printTagValues(sfile, fs, name, e.Key()); err != nil {
			return err
		}
	}
	fmt.Fprint(opt.Stdout, "\n")

	return nil
}

func (opt *Options) printTagValues(sfile *tsdb.SeriesFile, fs *tsi1.FileSet, name, key []byte) error {
	if !opt.showTagValues {
		return nil
	}

	// Iterate over each value.
	tw := tabwriter.NewWriter(opt.Stdout, 8, 8, 1, '\t', 0)
	itr := fs.TagValueIterator(name, key)
	for e := itr.Next(); e != nil; e = itr.Next() {
		if opt.tagValueFilter != nil && !opt.tagValueFilter.Match(e.Value()) {
			continue
		}

		fmt.Fprintf(tw, "        %s\t%v\n", e.Value(), deletedString(e.Deleted()))
		if err := tw.Flush(); err != nil {
			return fmt.Errorf("failed to flush tabwriter: %w", err)
		}

		if err := opt.printTagValueSeries(sfile, fs, name, key, e.Value()); err != nil {
			return err
		}
	}
	fmt.Fprint(opt.Stdout, "\n")

	return nil
}

func (opt *Options) printTagValueSeries(sfile *tsdb.SeriesFile, fs *tsi1.FileSet, name, key, value []byte) error {
	if !opt.showTagValueSeries {
		return nil
	}

	// Iterate over each series.
	tw := tabwriter.NewWriter(opt.Stdout, 8, 8, 1, '\t', 0)
	itr, err := fs.TagValueSeriesIDIterator(name, key, value)
	if err != nil {
		return fmt.Errorf("failed to get series ID iterator with name %q: %w", name, err)
	}
	for {
		e, err := itr.Next()
		if err != nil {
			return fmt.Errorf("failed to print tag value series: %w", err)
		} else if e.SeriesID == 0 {
			break
		}

		name, tags := tsdb.ParseSeriesKey(sfile.SeriesKey(e.SeriesID))

		if !opt.matchSeries(name, tags) {
			continue
		}

		fmt.Fprintf(tw, "            %s%s\n", name, tags.HashKey())
		if err := tw.Flush(); err != nil {
			return fmt.Errorf("failed to flush tabwriter: %w", err)
		}
	}
	fmt.Fprint(opt.Stdout, "\n")

	return nil
}

func (opt *Options) printFileSummaries(fs *tsi1.FileSet) error {
	for _, f := range fs.Files() {
		switch f := f.(type) {
		case *tsi1.LogFile:
			if err := opt.printLogFileSummary(f); err != nil {
				return err
			}
		case *tsi1.IndexFile:
			if err := opt.printIndexFileSummary(f); err != nil {
				return err
			}
		default:
			panic("unreachable")
		}
		fmt.Fprintln(opt.Stdout, "")
	}
	return nil
}

func (opt *Options) printLogFileSummary(f *tsi1.LogFile) error {
	fmt.Fprintf(opt.Stdout, "[LOG FILE] %s\n", filepath.Base(f.Path()))
	tw := tabwriter.NewWriter(opt.Stdout, 8, 8, 1, '\t', 0)
	fmt.Fprintf(tw, "Series:\t%d\n", f.SeriesN())
	fmt.Fprintf(tw, "Measurements:\t%d\n", f.MeasurementN())
	fmt.Fprintf(tw, "Tag Keys:\t%d\n", f.TagKeyN())
	fmt.Fprintf(tw, "Tag Values:\t%d\n", f.TagValueN())
	return tw.Flush()
}

func (opt *Options) printIndexFileSummary(f *tsi1.IndexFile) error {
	fmt.Fprintf(opt.Stdout, "[INDEX FILE] %s\n", filepath.Base(f.Path()))

	// Calculate summary stats.
	var measurementN, measurementSeriesN, measurementSeriesSize uint64
	var keyN uint64
	var valueN, valueSeriesN, valueSeriesSize uint64

	if mitr := f.MeasurementIterator(); mitr != nil {
		for me, _ := mitr.Next().(*tsi1.MeasurementBlockElem); me != nil; me, _ = mitr.Next().(*tsi1.MeasurementBlockElem) {
			kitr := f.TagKeyIterator(me.Name())
			for ke, _ := kitr.Next().(*tsi1.TagBlockKeyElem); ke != nil; ke, _ = kitr.Next().(*tsi1.TagBlockKeyElem) {
				vitr := f.TagValueIterator(me.Name(), ke.Key())
				for ve, _ := vitr.Next().(*tsi1.TagBlockValueElem); ve != nil; ve, _ = vitr.Next().(*tsi1.TagBlockValueElem) {
					valueN++
					valueSeriesN += uint64(ve.SeriesN())
					valueSeriesSize += uint64(len(ve.SeriesData()))
				}
				keyN++
			}
			measurementN++
			measurementSeriesN += uint64(me.SeriesN())
			measurementSeriesSize += uint64(len(me.SeriesData()))
		}
	}

	// Write stats.
	tw := tabwriter.NewWriter(opt.Stdout, 8, 8, 1, '\t', 0)
	fmt.Fprintf(tw, "Measurements:\t%d\n", measurementN)
	fmt.Fprintf(tw, "  Series data size:\t%d (%s)\n", measurementSeriesSize, formatSize(measurementSeriesSize))
	fmt.Fprintf(tw, "  Bytes per series:\t%.01fb\n", float64(measurementSeriesSize)/float64(measurementSeriesN))
	fmt.Fprintf(tw, "Tag Keys:\t%d\n", keyN)
	fmt.Fprintf(tw, "Tag Values:\t%d\n", valueN)
	fmt.Fprintf(tw, "  Series:\t%d\n", valueSeriesN)
	fmt.Fprintf(tw, "  Series data size:\t%d (%s)\n", valueSeriesSize, formatSize(valueSeriesSize))
	fmt.Fprintf(tw, "  Bytes per series:\t%.01fb\n", float64(valueSeriesSize)/float64(valueSeriesN))
	return tw.Flush()
}

// matchSeries returns true if the command filters matches the series.
func (opt *Options) matchSeries(name []byte, tags models.Tags) bool {
	// Filter by measurement.
	if opt.measurementFilter != nil && !opt.measurementFilter.Match(name) {
		return false
	}

	// Filter by tag key/value.
	if opt.tagKeyFilter != nil || opt.tagValueFilter != nil {
		var matched bool
		for _, tag := range tags {
			if (opt.tagKeyFilter == nil || opt.tagKeyFilter.Match(tag.Key)) && (opt.tagValueFilter == nil || opt.tagValueFilter.Match(tag.Value)) {
				matched = true
				break
			}
		}
		if !matched {
			return false
		}
	}

	return true
}

// deletedString returns "(deleted)" if v is true.
func deletedString(v bool) string {
	if v {
		return "(deleted)"
	}
	return ""
}

func formatSize(v uint64) string {
	denom := uint64(1)
	var uom string
	for _, uom = range []string{"b", "kb", "mb", "gb", "tb"} {
		if denom*1024 > v {
			break
		}
		denom *= 1024
	}
	return fmt.Sprintf("%0.01f%s", float64(v)/float64(denom), uom)
}
