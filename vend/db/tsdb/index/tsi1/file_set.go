package tsi1

import (
	"bytes"
	"fmt"
	"regexp"
	"sync"
	"unsafe"

	"github.com/cnosdb/cnosdb/vend/cnosql"
	"github.com/cnosdb/cnosdb/vend/db/pkg/estimator"package dumptsi

import (
	"errors"
	"fmt"
	errors2 "github.com/cnosdb/cnosdb/pkg/errors"
	"github.com/cnosdb/cnosdb/vend/db/logger"
	"github.com/cnosdb/cnosdb/vend/db/models"
	"github.com/cnosdb/cnosdb/vend/db/tsdb"
	_ "github.com/cnosdb/cnosdb/vend/db/tsdb/engine/tsm1"
	"github.com/cnosdb/cnosdb/vend/db/tsdb/index/tsi1"
	"github.com/spf13/cobra"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"text/tabwriter"
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
	sfile.Logger = logger.New(opt.Stderr)
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

	"github.com/cnosdb/cnosdb/vend/db/pkg/estimator/hll"
	"github.com/cnosdb/cnosdb/vend/db/tsdb"
)

// FileSet represents a collection of files.
type FileSet struct {
	levels       []CompactionLevel
	sfile        *tsdb.SeriesFile
	files        []File
	manifestSize int64 // Size of the manifest file in bytes.
}

// NewFileSet returns a new instance of FileSet.
func NewFileSet(levels []CompactionLevel, sfile *tsdb.SeriesFile, files []File) (*FileSet, error) {
	return &FileSet{
		levels: levels,
		sfile:  sfile,
		files:  files,
	}, nil
}

// bytes estimates the memory footprint of this FileSet, in bytes.
func (fs *FileSet) bytes() int {
	var b int
	for _, level := range fs.levels {
		b += int(unsafe.Sizeof(level))
	}
	// Do not count SeriesFile because it belongs to the code that constructed this FileSet.
	for _, file := range fs.files {
		b += file.bytes()
	}
	b += int(unsafe.Sizeof(*fs))
	return b
}

// Close closes all the files in the file set.
func (fs FileSet) Close() error {
	var err error
	for _, f := range fs.files {
		if e := f.Close(); e != nil && err == nil {
			err = e
		}
	}
	return err
}

// Retain adds a reference count to all files.
func (fs *FileSet) Retain() {
	for _, f := range fs.files {
		f.Retain()
	}
}

// Release removes a reference count from all files.
func (fs *FileSet) Release() {
	for _, f := range fs.files {
		f.Release()
	}
}

// SeriesFile returns the attached series file.
func (fs *FileSet) SeriesFile() *tsdb.SeriesFile { return fs.sfile }

// PrependLogFile returns a new file set with f added at the beginning.
// Filters do not need to be rebuilt because log files have no bloom filter.
func (fs *FileSet) PrependLogFile(f *LogFile) *FileSet {
	return &FileSet{
		levels: fs.levels,
		sfile:  fs.sfile,
		files:  append([]File{f}, fs.files...),
	}
}

// Size returns the on-disk size of the FileSet.
func (fs *FileSet) Size() int64 {
	var total int64
	for _, f := range fs.files {
		total += f.Size()
	}
	return total + int64(fs.manifestSize)
}

// MustReplace swaps a list of files for a single file and returns a new file set.
// The caller should always guarantee that the files exist and are contiguous.
func (fs *FileSet) MustReplace(oldFiles []File, newFile File) *FileSet {
	assert(len(oldFiles) > 0, "cannot replace empty files")

	// Find index of first old file.
	var i int
	for ; i < len(fs.files); i++ {
		if fs.files[i] == oldFiles[0] {
			break
		} else if i == len(fs.files)-1 {
			panic("first replacement file not found")
		}
	}

	// Ensure all old files are contiguous.
	for j := range oldFiles {
		if fs.files[i+j] != oldFiles[j] {
			panic(fmt.Sprintf("cannot replace non-contiguous files: subset=%+v, fileset=%+v", Files(oldFiles).IDs(), Files(fs.files).IDs()))
		}
	}

	// Copy to new fileset.
	other := make([]File, len(fs.files)-len(oldFiles)+1)
	copy(other[:i], fs.files[:i])
	other[i] = newFile
	copy(other[i+1:], fs.files[i+len(oldFiles):])

	// Build new fileset and rebuild changed filters.
	return &FileSet{
		levels: fs.levels,
		files:  other,
	}
}

// MaxID returns the highest file identifier.
func (fs *FileSet) MaxID() int {
	var max int
	for _, f := range fs.files {
		if i := f.ID(); i > max {
			max = i
		}
	}
	return max
}

// Files returns all files in the set.
func (fs *FileSet) Files() []File {
	return fs.files
}

// LogFiles returns all log files from the file set.
func (fs *FileSet) LogFiles() []*LogFile {
	var a []*LogFile
	for _, f := range fs.files {
		if f, ok := f.(*LogFile); ok {
			a = append(a, f)
		}
	}
	return a
}

// IndexFiles returns all index files from the file set.
func (fs *FileSet) IndexFiles() []*IndexFile {
	var a []*IndexFile
	for _, f := range fs.files {
		if f, ok := f.(*IndexFile); ok {
			a = append(a, f)
		}
	}
	return a
}

// LastContiguousIndexFilesByLevel returns the last contiguous files by level.
// These can be used by the compaction scheduler.
func (fs *FileSet) LastContiguousIndexFilesByLevel(level int) []*IndexFile {
	if level == 0 {
		return nil
	}

	var a []*IndexFile
	for i := len(fs.files) - 1; i >= 0; i-- {
		f := fs.files[i]

		// Ignore files above level, stop on files below level.
		if level < f.Level() {
			continue
		} else if level > f.Level() {
			break
		}

		a = append([]*IndexFile{f.(*IndexFile)}, a...)
	}
	return a
}

// Measurement returns a measurement by name.
func (fs *FileSet) Measurement(name []byte) MeasurementElem {
	for _, f := range fs.files {
		if e := f.Measurement(name); e == nil {
			continue
		} else if e.Deleted() {
			return nil
		} else {
			return e
		}
	}
	return nil
}

// MeasurementIterator returns an iterator over all measurements in the index.
func (fs *FileSet) MeasurementIterator() MeasurementIterator {
	a := make([]MeasurementIterator, 0, len(fs.files))
	for _, f := range fs.files {
		itr := f.MeasurementIterator()
		if itr != nil {
			a = append(a, itr)
		}
	}
	return MergeMeasurementIterators(a...)
}

// TagKeyIterator returns an iterator over all tag keys for a measurement.
func (fs *FileSet) TagKeyIterator(name []byte) TagKeyIterator {
	a := make([]TagKeyIterator, 0, len(fs.files))
	for _, f := range fs.files {
		itr := f.TagKeyIterator(name)
		if itr != nil {
			a = append(a, itr)
		}
	}
	return MergeTagKeyIterators(a...)
}

// MeasurementSeriesIDIterator returns a series iterator for a measurement.
func (fs *FileSet) MeasurementSeriesIDIterator(name []byte) tsdb.SeriesIDIterator {
	a := make([]tsdb.SeriesIDIterator, 0, len(fs.files))
	for _, f := range fs.files {
		itr := f.MeasurementSeriesIDIterator(name)
		if itr != nil {
			a = append(a, itr)
		}
	}
	return tsdb.MergeSeriesIDIterators(a...)
}

// MeasurementTagKeysByExpr extracts the tag keys wanted by the expression.
func (fs *FileSet) MeasurementTagKeysByExpr(name []byte, expr cnosql.Expr) (map[string]struct{}, error) {
	// Return all keys if no condition was passed in.
	if expr == nil {
		m := make(map[string]struct{})
		if itr := fs.TagKeyIterator(name); itr != nil {
			for e := itr.Next(); e != nil; e = itr.Next() {
				m[string(e.Key())] = struct{}{}
			}
		}
		return m, nil
	}

	switch e := expr.(type) {
	case *cnosql.BinaryExpr:
		switch e.Op {
		case cnosql.EQ, cnosql.NEQ, cnosql.EQREGEX, cnosql.NEQREGEX:
			tag, ok := e.LHS.(*cnosql.VarRef)
			if !ok {
				return nil, fmt.Errorf("left side of '%s' must be a tag key", e.Op.String())
			} else if tag.Val != "_tagKey" {
				return nil, nil
			}

			if cnosql.IsRegexOp(e.Op) {
				re, ok := e.RHS.(*cnosql.RegexLiteral)
				if !ok {
					return nil, fmt.Errorf("right side of '%s' must be a regular expression", e.Op.String())
				}
				return fs.tagKeysByFilter(name, e.Op, nil, re.Val), nil
			}

			s, ok := e.RHS.(*cnosql.StringLiteral)
			if !ok {
				return nil, fmt.Errorf("right side of '%s' must be a tag value string", e.Op.String())
			}
			return fs.tagKeysByFilter(name, e.Op, []byte(s.Val), nil), nil

		case cnosql.AND, cnosql.OR:
			lhs, err := fs.MeasurementTagKeysByExpr(name, e.LHS)
			if err != nil {
				return nil, err
			}

			rhs, err := fs.MeasurementTagKeysByExpr(name, e.RHS)
			if err != nil {
				return nil, err
			}

			if lhs != nil && rhs != nil {
				if e.Op == cnosql.OR {
					return unionStringSets(lhs, rhs), nil
				}
				return intersectStringSets(lhs, rhs), nil
			} else if lhs != nil {
				return lhs, nil
			} else if rhs != nil {
				return rhs, nil
			}
			return nil, nil
		default:
			return nil, fmt.Errorf("invalid operator")
		}

	case *cnosql.ParenExpr:
		return fs.MeasurementTagKeysByExpr(name, e.Expr)
	}

	return nil, fmt.Errorf("%#v", expr)
}

// tagKeysByFilter will filter the tag keys for the measurement.
func (fs *FileSet) tagKeysByFilter(name []byte, op cnosql.Token, val []byte, regex *regexp.Regexp) map[string]struct{} {
	ss := make(map[string]struct{})
	itr := fs.TagKeyIterator(name)
	if itr != nil {
		for e := itr.Next(); e != nil; e = itr.Next() {
			var matched bool
			switch op {
			case cnosql.EQ:
				matched = bytes.Equal(e.Key(), val)
			case cnosql.NEQ:
				matched = !bytes.Equal(e.Key(), val)
			case cnosql.EQREGEX:
				matched = regex.Match(e.Key())
			case cnosql.NEQREGEX:
				matched = !regex.Match(e.Key())
			}

			if !matched {
				continue
			}
			ss[string(e.Key())] = struct{}{}
		}
	}
	return ss
}

// TagKeySeriesIDIterator returns a series iterator for all values across a single key.
func (fs *FileSet) TagKeySeriesIDIterator(name, key []byte) (tsdb.SeriesIDIterator, error) {
	a := make([]tsdb.SeriesIDIterator, 0, len(fs.files))
	for _, f := range fs.files {
		itr, err := f.TagKeySeriesIDIterator(name, key)
		if err != nil {
			return nil, err
		} else if itr != nil {
			a = append(a, itr)
		}
	}
	return tsdb.MergeSeriesIDIterators(a...), nil
}

// HasTagKey returns true if the tag key exists.
func (fs *FileSet) HasTagKey(name, key []byte) bool {
	for _, f := range fs.files {
		if e := f.TagKey(name, key); e != nil {
			return !e.Deleted()
		}
	}
	return false
}

// HasTagValue returns true if the tag value exists.
func (fs *FileSet) HasTagValue(name, key, value []byte) bool {
	for _, f := range fs.files {
		if e := f.TagValue(name, key, value); e != nil {
			return !e.Deleted()
		}
	}
	return false
}

// TagValueIterator returns a value iterator for a tag key.
func (fs *FileSet) TagValueIterator(name, key []byte) TagValueIterator {
	a := make([]TagValueIterator, 0, len(fs.files))
	for _, f := range fs.files {
		itr := f.TagValueIterator(name, key)
		if itr != nil {
			a = append(a, itr)
		}
	}
	return MergeTagValueIterators(a...)
}

// TagValueSeriesIDIterator returns a series iterator for a single tag value.
func (fs *FileSet) TagValueSeriesIDIterator(name, key, value []byte) (tsdb.SeriesIDIterator, error) {
	ss := tsdb.NewSeriesIDSet()

	var ftss *tsdb.SeriesIDSet
	for i := len(fs.files) - 1; i >= 0; i-- {
		f := fs.files[i]

		// Remove tombstones set in previous file.
		if ftss != nil && ftss.Cardinality() > 0 {
			ss = ss.AndNot(ftss)
		}

		// Fetch tag value series set for this file and merge into overall set.
		fss, err := f.TagValueSeriesIDSet(name, key, value)
		if err != nil {
			return nil, err
		} else if fss != nil {
			ss.Merge(fss)
		}

		// Fetch tombstone set to be processed on next file.
		if ftss, err = f.TombstoneSeriesIDSet(); err != nil {
			return nil, err
		}
	}
	return tsdb.NewSeriesIDSetIterator(ss), nil
}

// MeasurementsSketches returns the merged measurement sketches for the FileSet.
func (fs *FileSet) MeasurementsSketches() (estimator.Sketch, estimator.Sketch, error) {
	sketch, tSketch := hll.NewDefaultPlus(), hll.NewDefaultPlus()
	for _, f := range fs.files {
		if s, t, err := f.MeasurementsSketches(); err != nil {
			return nil, nil, err
		} else if err := sketch.Merge(s); err != nil {
			return nil, nil, err
		} else if err := tSketch.Merge(t); err != nil {
			return nil, nil, err
		}
	}
	return sketch, tSketch, nil
}

// SeriesSketches returns the merged measurement sketches for the FileSet.
func (fs *FileSet) SeriesSketches() (estimator.Sketch, estimator.Sketch, error) {
	sketch, tSketch := hll.NewDefaultPlus(), hll.NewDefaultPlus()
	for _, f := range fs.files {
		if s, t, err := f.SeriesSketches(); err != nil {
			return nil, nil, err
		} else if err := sketch.Merge(s); err != nil {
			return nil, nil, err
		} else if err := tSketch.Merge(t); err != nil {
			return nil, nil, err
		}
	}
	return sketch, tSketch, nil
}

// File represents a log or index file.
type File interface {
	Close() error
	Path() string

	ID() int
	Level() int

	Measurement(name []byte) MeasurementElem
	MeasurementIterator() MeasurementIterator
	MeasurementHasSeries(ss *tsdb.SeriesIDSet, name []byte) bool

	TagKey(name, key []byte) TagKeyElem
	TagKeyIterator(name []byte) TagKeyIterator

	TagValue(name, key, value []byte) TagValueElem
	TagValueIterator(name, key []byte) TagValueIterator

	// Series iteration.
	MeasurementSeriesIDIterator(name []byte) tsdb.SeriesIDIterator
	TagKeySeriesIDIterator(name, key []byte) (tsdb.SeriesIDIterator, error)
	TagValueSeriesIDSet(name, key, value []byte) (*tsdb.SeriesIDSet, error)

	// Sketches for cardinality estimation
	MeasurementsSketches() (s, t estimator.Sketch, err error)
	SeriesSketches() (s, t estimator.Sketch, err error)

	// Bitmap series existance.
	SeriesIDSet() (*tsdb.SeriesIDSet, error)
	TombstoneSeriesIDSet() (*tsdb.SeriesIDSet, error)

	// Reference counting.
	Retain()
	Release()

	// Size of file on disk
	Size() int64

	// Estimated memory footprint
	bytes() int
}

type Files []File

func (a Files) IDs() []int {
	ids := make([]int, len(a))
	for i := range a {
		ids[i] = a[i].ID()
	}
	return ids
}

// fileSetSeriesIDIterator attaches a fileset to an iterator that is released on close.
type fileSetSeriesIDIterator struct {
	once sync.Once
	fs   *FileSet
	itr  tsdb.SeriesIDIterator
}

func newFileSetSeriesIDIterator(fs *FileSet, itr tsdb.SeriesIDIterator) tsdb.SeriesIDIterator {
	if itr == nil {
		fs.Release()
		return nil
	}
	if itr, ok := itr.(tsdb.SeriesIDSetIterator); ok {
		return &fileSetSeriesIDSetIterator{fs: fs, itr: itr}
	}
	return &fileSetSeriesIDIterator{fs: fs, itr: itr}
}

func (itr *fileSetSeriesIDIterator) Next() (tsdb.SeriesIDElem, error) {
	return itr.itr.Next()
}

func (itr *fileSetSeriesIDIterator) Close() error {
	itr.once.Do(func() { itr.fs.Release() })
	return itr.itr.Close()
}

// fileSetSeriesIDSetIterator attaches a fileset to an iterator that is released on close.
type fileSetSeriesIDSetIterator struct {
	once sync.Once
	fs   *FileSet
	itr  tsdb.SeriesIDSetIterator
}

func (itr *fileSetSeriesIDSetIterator) Next() (tsdb.SeriesIDElem, error) {
	return itr.itr.Next()
}

func (itr *fileSetSeriesIDSetIterator) Close() error {
	itr.once.Do(func() { itr.fs.Release() })
	return itr.itr.Close()
}

func (itr *fileSetSeriesIDSetIterator) SeriesIDSet() *tsdb.SeriesIDSet {
	return itr.itr.SeriesIDSet()
}

// fileSetMeasurementIterator attaches a fileset to an iterator that is released on close.
type fileSetMeasurementIterator struct {
	once sync.Once
	fs   *FileSet
	itr  tsdb.MeasurementIterator
}

func newFileSetMeasurementIterator(fs *FileSet, itr tsdb.MeasurementIterator) *fileSetMeasurementIterator {
	return &fileSetMeasurementIterator{fs: fs, itr: itr}
}

func (itr *fileSetMeasurementIterator) Next() ([]byte, error) {
	return itr.itr.Next()
}

func (itr *fileSetMeasurementIterator) Close() error {
	itr.once.Do(func() { itr.fs.Release() })
	return itr.itr.Close()
}

// fileSetTagKeyIterator attaches a fileset to an iterator that is released on close.
type fileSetTagKeyIterator struct {
	once sync.Once
	fs   *FileSet
	itr  tsdb.TagKeyIterator
}

func newFileSetTagKeyIterator(fs *FileSet, itr tsdb.TagKeyIterator) *fileSetTagKeyIterator {
	return &fileSetTagKeyIterator{fs: fs, itr: itr}
}

func (itr *fileSetTagKeyIterator) Next() ([]byte, error) {
	return itr.itr.Next()
}

func (itr *fileSetTagKeyIterator) Close() error {
	itr.once.Do(func() { itr.fs.Release() })
	return itr.itr.Close()
}

// fileSetTagValueIterator attaches a fileset to an iterator that is released on close.
type fileSetTagValueIterator struct {
	once sync.Once
	fs   *FileSet
	itr  tsdb.TagValueIterator
}

func newFileSetTagValueIterator(fs *FileSet, itr tsdb.TagValueIterator) *fileSetTagValueIterator {
	return &fileSetTagValueIterator{fs: fs, itr: itr}
}

func (itr *fileSetTagValueIterator) Next() ([]byte, error) {
	return itr.itr.Next()
}

func (itr *fileSetTagValueIterator) Close() error {
	itr.once.Do(func() { itr.fs.Release() })
	return itr.itr.Close()
}
