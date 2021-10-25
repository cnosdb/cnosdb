package tsi1

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sync"
	"unsafe"

	"github.com/cnosdatabase/db/models"
	"github.com/cnosdatabase/db/pkg/estimator"
	"github.com/cnosdatabase/db/pkg/estimator/hll"
	"github.com/cnosdatabase/db/pkg/mmap"
	"github.com/cnosdatabase/db/tsdb"
)

// IndexFileVersion is the current TSI1 index file version.
const IndexFileVersion = 1

// FileSignature represents a magic number at the header of the index file.
const FileSignature = "TSI1"

// IndexFile field size constants.
const (
	// IndexFile trailer fields
	IndexFileVersionSize = 2

	// IndexFileTrailerSize is the size of the trailer. Currently 82 bytes.
	IndexFileTrailerSize = IndexFileVersionSize +
		8 + 8 + // metric block offset + size
		8 + 8 + // series id set offset + size
		8 + 8 + // tombstone series id set offset + size
		8 + 8 + // series sketch offset + size
		8 + 8 + // tombstone series sketch offset + size
		0
)

// IndexFile errors.
var (
	ErrInvalidIndexFile            = errors.New("invalid index file")
	ErrUnsupportedIndexFileVersion = errors.New("unsupported index file version")
)

// IndexFile represents a collection of metric, tag, and series data.
type IndexFile struct {
	wg   sync.WaitGroup // ref count
	data []byte

	// Components
	sfile *tsdb.SeriesFile
	tblks map[string]*TagBlock // tag blocks by metric name
	mblk  MetricBlock

	// Raw series set data.
	seriesIDSetData          []byte
	tombstoneSeriesIDSetData []byte

	// Series sketch data.
	sketchData, tSketchData []byte

	// Sortable identifier & filepath to the log file.
	level int
	id    int

	mu sync.RWMutex
	// Compaction tracking.
	compacting bool

	// Path to data file.
	path string
}

// NewIndexFile returns a new instance of IndexFile.
func NewIndexFile(sfile *tsdb.SeriesFile) *IndexFile {
	return &IndexFile{
		sfile: sfile,
	}
}

// bytes estimates the memory footprint of this IndexFile, in bytes.
func (f *IndexFile) bytes() int {
	var b int
	f.wg.Add(1)
	b += 16 // wg WaitGroup is 16 bytes
	b += int(unsafe.Sizeof(f.data))
	// Do not count f.data contents because it is mmap'd
	b += int(unsafe.Sizeof(f.sfile))
	// Do not count SeriesFile because it belongs to the code that constructed this IndexFile.
	b += int(unsafe.Sizeof(f.tblks))
	for k, v := range f.tblks {
		// Do not count TagBlock contents, they all reference f.data
		b += int(unsafe.Sizeof(k)) + len(k)
		b += int(unsafe.Sizeof(*v))
	}
	b += int(unsafe.Sizeof(f.mblk)) + f.mblk.bytes()
	b += int(unsafe.Sizeof(f.seriesIDSetData) + unsafe.Sizeof(f.tombstoneSeriesIDSetData))
	// Do not count contents of seriesIDSetData or tombstoneSeriesIDSetData: references f.data
	b += int(unsafe.Sizeof(f.level) + unsafe.Sizeof(f.id))
	b += 24 // mu RWMutex is 24 bytes
	b += int(unsafe.Sizeof(f.compacting))
	b += int(unsafe.Sizeof(f.path)) + len(f.path)
	f.wg.Done()
	return b
}

// Open memory maps the data file at the file's path.
func (f *IndexFile) Open() error {
	defer func() {
		if err := recover(); err != nil {
			err = fmt.Errorf("[Index file: %s] %v", f.path, err)
			panic(err)
		}
	}()

	// Extract identifier from path name.
	f.id, f.level = ParseFilename(f.Path())

	data, err := mmap.Map(f.Path(), 0)
	if err != nil {
		return err
	}

	return f.UnmarshalBinary(data)
}

// Close unmaps the data file.
func (f *IndexFile) Close() error {
	// Wait until all references are released.
	f.wg.Wait()

	f.sfile = nil
	f.tblks = nil
	f.mblk = MetricBlock{}
	return mmap.Unmap(f.data)
}

// ID returns the file sequence identifier.
func (f *IndexFile) ID() int { return f.id }

// Path returns the file path.
func (f *IndexFile) Path() string { return f.path }

// SetPath sets the file's path.
func (f *IndexFile) SetPath(path string) { f.path = path }

// Level returns the compaction level for the file.
func (f *IndexFile) Level() int { return f.level }

// Retain adds a reference count to the file.
func (f *IndexFile) Retain() { f.wg.Add(1) }

// Release removes a reference count from the file.
func (f *IndexFile) Release() { f.wg.Done() }

// Size returns the size of the index file, in bytes.
func (f *IndexFile) Size() int64 { return int64(len(f.data)) }

// Compacting returns true if the file is being compacted.
func (f *IndexFile) Compacting() bool {
	f.mu.RLock()
	v := f.compacting
	f.mu.RUnlock()
	return v
}

// UnmarshalBinary opens an index from data.
// The byte slice is retained so it must be kept open.
func (f *IndexFile) UnmarshalBinary(data []byte) error {
	// Ensure magic number exists at the beginning.
	if len(data) < len(FileSignature) {
		return io.ErrShortBuffer
	} else if !bytes.Equal(data[:len(FileSignature)], []byte(FileSignature)) {
		return ErrInvalidIndexFile
	}

	// Read index file trailer.
	t, err := ReadIndexFileTrailer(data)
	if err != nil {
		return err
	}

	// Slice series sketch data.
	f.sketchData = data[t.SeriesSketch.Offset : t.SeriesSketch.Offset+t.SeriesSketch.Size]
	f.tSketchData = data[t.TombstoneSeriesSketch.Offset : t.TombstoneSeriesSketch.Offset+t.TombstoneSeriesSketch.Size]

	// Slice series set data.
	f.seriesIDSetData = data[t.SeriesIDSet.Offset : t.SeriesIDSet.Offset+t.SeriesIDSet.Size]
	f.tombstoneSeriesIDSetData = data[t.TombstoneSeriesIDSet.Offset : t.TombstoneSeriesIDSet.Offset+t.TombstoneSeriesIDSet.Size]

	// Unmarshal metric block.
	if err := f.mblk.UnmarshalBinary(data[t.MetricBlock.Offset:][:t.MetricBlock.Size]); err != nil {
		return err
	}

	// Unmarshal each tag block.
	f.tblks = make(map[string]*TagBlock)
	itr := f.mblk.Iterator()

	for m := itr.Next(); m != nil; m = itr.Next() {
		e := m.(*MetricBlockElem)

		// Slice metric block data.
		buf := data[e.tagBlock.offset:]
		buf = buf[:e.tagBlock.size]

		// Unmarshal metric block.
		var tblk TagBlock
		if err := tblk.UnmarshalBinary(buf); err != nil {
			return err
		}
		f.tblks[string(e.name)] = &tblk
	}

	// Save reference to entire data block.
	f.data = data

	return nil
}

func (f *IndexFile) SeriesIDSet() (*tsdb.SeriesIDSet, error) {
	ss := tsdb.NewSeriesIDSet()
	if err := ss.UnmarshalBinary(f.seriesIDSetData); err != nil {
		return nil, err
	}
	return ss, nil
}

func (f *IndexFile) TombstoneSeriesIDSet() (*tsdb.SeriesIDSet, error) {
	ss := tsdb.NewSeriesIDSet()
	if err := ss.UnmarshalBinaryUnsafe(f.tombstoneSeriesIDSetData); err != nil {
		return nil, err
	}
	return ss, nil
}

// Metric returns a metric element.
func (f *IndexFile) Metric(name []byte) MetricElem {
	e, ok := f.mblk.Elem(name)
	if !ok {
		return nil
	}
	return &e
}

// MetricN returns the number of metrics in the file.
func (f *IndexFile) MetricN() (n uint64) {
	mitr := f.mblk.Iterator()
	for me := mitr.Next(); me != nil; me = mitr.Next() {
		n++
	}
	return n
}

// MetricHasSeries returns true if a metric has any non-tombstoned series.
func (f *IndexFile) MetricHasSeries(ss *tsdb.SeriesIDSet, name []byte) (ok bool) {
	e, ok := f.mblk.Elem(name)
	if !ok {
		return false
	}

	var exists bool
	e.ForEachSeriesID(func(id uint64) error {
		if ss.Contains(id) {
			exists = true
			return errors.New("done")
		}
		return nil
	})
	return exists
}

// TagValueIterator returns a value iterator for a tag key and a flag
// indicating if a tombstone exists on the metric or key.
func (f *IndexFile) TagValueIterator(name, key []byte) TagValueIterator {
	tblk := f.tblks[string(name)]
	if tblk == nil {
		return nil
	}

	// Find key element.
	ke := tblk.TagKeyElem(key)
	if ke == nil {
		return nil
	}

	// Merge all value series iterators together.
	return ke.TagValueIterator()
}

// TagKeySeriesIDIterator returns a series iterator for a tag key and a flag
// indicating if a tombstone exists on the metric or key.
func (f *IndexFile) TagKeySeriesIDIterator(name, key []byte) (tsdb.SeriesIDIterator, error) {
	tblk := f.tblks[string(name)]
	if tblk == nil {
		return nil, nil
	}

	// Find key element.
	ke := tblk.TagKeyElem(key)
	if ke == nil {
		return nil, nil
	}

	// Merge all value series iterators together.
	vitr := ke.TagValueIterator()

	var itrs []tsdb.SeriesIDIterator
	for ve := vitr.Next(); ve != nil; ve = vitr.Next() {
		tblk, ok := ve.(*TagBlockValueElem)
		if !ok {
			return nil, fmt.Errorf("got type %T for iterator, expected %T", ve, TagBlockValueElem{})
		}

		ss, err := tblk.SeriesIDSet()
		if err != nil {
			return nil, err
		}
		itrs = append(itrs, tsdb.NewSeriesIDSetIterator(ss))
	}

	return tsdb.MergeSeriesIDIterators(itrs...), nil
}

// TagValueSeriesIDSet returns a series id set for a tag value.
func (f *IndexFile) TagValueSeriesIDSet(name, key, value []byte) (*tsdb.SeriesIDSet, error) {
	tblk := f.tblks[string(name)]
	if tblk == nil {
		return nil, nil
	}

	// Find value element.
	var valueElem TagBlockValueElem
	if !tblk.DecodeTagValueElem(key, value, &valueElem) {
		return nil, nil
	} else if valueElem.SeriesN() == 0 {
		return nil, nil
	}
	return valueElem.SeriesIDSet()
}

// TagKey returns a tag key.
func (f *IndexFile) TagKey(name, key []byte) TagKeyElem {
	tblk := f.tblks[string(name)]
	if tblk == nil {
		return nil
	}
	return tblk.TagKeyElem(key)
}

// TagValue returns a tag value.
func (f *IndexFile) TagValue(name, key, value []byte) TagValueElem {
	tblk := f.tblks[string(name)]
	if tblk == nil {
		return nil
	}
	return tblk.TagValueElem(key, value)
}

// HasSeries returns flags indicating if the series exists and if it is tombstoned.
func (f *IndexFile) HasSeries(name []byte, tags models.Tags, buf []byte) (exists, tombstoned bool) {
	return f.sfile.HasSeries(name, tags, buf), false // TODO: series tombstone
}

// TagValueElem returns an element for a metric/tag/value.
func (f *IndexFile) TagValueElem(name, key, value []byte) TagValueElem {
	tblk, ok := f.tblks[string(name)]
	if !ok {
		return nil
	}
	return tblk.TagValueElem(key, value)
}

// MetricIterator returns an iterator over all metrics.
func (f *IndexFile) MetricIterator() MetricIterator {
	return f.mblk.Iterator()
}

// TagKeyIterator returns an iterator over all tag keys for a metric.
func (f *IndexFile) TagKeyIterator(name []byte) TagKeyIterator {
	blk := f.tblks[string(name)]
	if blk == nil {
		return nil
	}
	return blk.TagKeyIterator()
}

// MetricSeriesIDIterator returns an iterator over a metric's series.
func (f *IndexFile) MetricSeriesIDIterator(name []byte) tsdb.SeriesIDIterator {
	return f.mblk.SeriesIDIterator(name)
}

// MetricsSketches returns existence and tombstone sketches for metrics.
func (f *IndexFile) MetricsSketches() (sketch, tSketch estimator.Sketch, err error) {
	return f.mblk.Sketches()
}

// SeriesSketches returns existence and tombstone sketches for series.
func (f *IndexFile) SeriesSketches() (sketch, tSketch estimator.Sketch, err error) {
	sketch = hll.NewDefaultPlus()
	if err := sketch.UnmarshalBinary(f.sketchData); err != nil {
		return nil, nil, err
	}

	tSketch = hll.NewDefaultPlus()
	if err := tSketch.UnmarshalBinary(f.tSketchData); err != nil {
		return nil, nil, err
	}
	return sketch, tSketch, nil
}

// ReadIndexFileTrailer returns the index file trailer from data.
func ReadIndexFileTrailer(data []byte) (IndexFileTrailer, error) {
	var t IndexFileTrailer

	// Read version.
	t.Version = int(binary.BigEndian.Uint16(data[len(data)-IndexFileVersionSize:]))
	if t.Version != IndexFileVersion {
		return t, ErrUnsupportedIndexFileVersion
	}

	// Slice trailer data.
	buf := data[len(data)-IndexFileTrailerSize:]

	// Read metric block info.
	t.MetricBlock.Offset, buf = int64(binary.BigEndian.Uint64(buf[0:8])), buf[8:]
	t.MetricBlock.Size, buf = int64(binary.BigEndian.Uint64(buf[0:8])), buf[8:]

	// Read series id set info.
	t.SeriesIDSet.Offset, buf = int64(binary.BigEndian.Uint64(buf[0:8])), buf[8:]
	t.SeriesIDSet.Size, buf = int64(binary.BigEndian.Uint64(buf[0:8])), buf[8:]

	// Read series tombstone id set info.
	t.TombstoneSeriesIDSet.Offset, buf = int64(binary.BigEndian.Uint64(buf[0:8])), buf[8:]
	t.TombstoneSeriesIDSet.Size, buf = int64(binary.BigEndian.Uint64(buf[0:8])), buf[8:]

	// Read series sketch set info.
	t.SeriesSketch.Offset, buf = int64(binary.BigEndian.Uint64(buf[0:8])), buf[8:]
	t.SeriesSketch.Size, buf = int64(binary.BigEndian.Uint64(buf[0:8])), buf[8:]

	// Read series tombstone sketch info.
	t.TombstoneSeriesSketch.Offset, buf = int64(binary.BigEndian.Uint64(buf[0:8])), buf[8:]
	t.TombstoneSeriesSketch.Size, buf = int64(binary.BigEndian.Uint64(buf[0:8])), buf[8:]

	if len(buf) != 2 { // Version field still in buffer.
		return t, fmt.Errorf("unread %d bytes left unread in trailer", len(buf)-2)
	}
	return t, nil
}

// IndexFileTrailer represents meta data written to the end of the index file.
type IndexFileTrailer struct {
	Version int

	MetricBlock struct {
		Offset int64
		Size   int64
	}

	SeriesIDSet struct {
		Offset int64
		Size   int64
	}

	TombstoneSeriesIDSet struct {
		Offset int64
		Size   int64
	}

	SeriesSketch struct {
		Offset int64
		Size   int64
	}

	TombstoneSeriesSketch struct {
		Offset int64
		Size   int64
	}
}

// WriteTo writes the trailer to w.
func (t *IndexFileTrailer) WriteTo(w io.Writer) (n int64, err error) {
	// Write metric block info.
	if err := writeUint64To(w, uint64(t.MetricBlock.Offset), &n); err != nil {
		return n, err
	} else if err := writeUint64To(w, uint64(t.MetricBlock.Size), &n); err != nil {
		return n, err
	}

	// Write series id set info.
	if err := writeUint64To(w, uint64(t.SeriesIDSet.Offset), &n); err != nil {
		return n, err
	} else if err := writeUint64To(w, uint64(t.SeriesIDSet.Size), &n); err != nil {
		return n, err
	}

	// Write tombstone series id set info.
	if err := writeUint64To(w, uint64(t.TombstoneSeriesIDSet.Offset), &n); err != nil {
		return n, err
	} else if err := writeUint64To(w, uint64(t.TombstoneSeriesIDSet.Size), &n); err != nil {
		return n, err
	}

	// Write series sketch info.
	if err := writeUint64To(w, uint64(t.SeriesSketch.Offset), &n); err != nil {
		return n, err
	} else if err := writeUint64To(w, uint64(t.SeriesSketch.Size), &n); err != nil {
		return n, err
	}

	// Write series tombstone sketch info.
	if err := writeUint64To(w, uint64(t.TombstoneSeriesSketch.Offset), &n); err != nil {
		return n, err
	} else if err := writeUint64To(w, uint64(t.TombstoneSeriesSketch.Size), &n); err != nil {
		return n, err
	}

	// Write index file encoding version.
	if err := writeUint16To(w, IndexFileVersion, &n); err != nil {
		return n, err
	}

	return n, nil
}

// FormatIndexFileName generates an index filename for the given index.
func FormatIndexFileName(id, level int) string {
	return fmt.Sprintf("L%d-%08d%s", level, id, IndexFileExt)
}
