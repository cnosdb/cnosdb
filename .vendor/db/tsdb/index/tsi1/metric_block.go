package tsi1

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"sort"
	"unsafe"

	"github.com/cnosdatabase/db/pkg/estimator"
	"github.com/cnosdatabase/db/pkg/estimator/hll"
	"github.com/cnosdatabase/db/pkg/rhh"
	"github.com/cnosdatabase/db/tsdb"
)

// MetricBlockVersion is the version of the metric block.
const MetricBlockVersion = 1

// Metric flag constants.
const (
	MetricTombstoneFlag   = 0x01
	MetricSeriesIDSetFlag = 0x02
)

// Metric field size constants.
const (
	// 1 byte offset for the block to ensure non-zero offsets.
	MetricFillSize = 1

	// Metric trailer fields
	MetricTrailerSize = 0 +
		2 + // version
		8 + 8 + // data offset/size
		8 + 8 + // hash index offset/size
		8 + 8 + // metric sketch offset/size
		8 + 8 // tombstone metric sketch offset/size

	// Metric key block fields.
	MetricNSize      = 8
	MetricOffsetSize = 8

	SeriesIDSize = 8
)

// Metric errors.
var (
	ErrUnsupportedMetricBlockVersion = errors.New("unsupported metric block version")
	ErrMetricBlockSizeMismatch       = errors.New("metric block size mismatch")
)

// MetricBlock represents a collection of all metrics in an index.
type MetricBlock struct {
	data     []byte
	hashData []byte

	// Metric sketch and tombstone sketch for cardinality estimation.
	sketchData, tSketchData []byte

	version int // block version
}

// bytes estimates the memory footprint of this MetricBlock, in bytes.
func (blk *MetricBlock) bytes() int {
	var b int
	// Do not count contents of blk.data or blk.hashData because they reference into an external []byte
	b += int(unsafe.Sizeof(*blk))
	return b
}

// Version returns the encoding version parsed from the data.
// Only valid after UnmarshalBinary() has been successfully invoked.
func (blk *MetricBlock) Version() int { return blk.version }

// Elem returns an element for a metric.
func (blk *MetricBlock) Elem(name []byte) (e MetricBlockElem, ok bool) {
	n := int64(binary.BigEndian.Uint64(blk.hashData[:MetricNSize]))
	hash := rhh.HashKey(name)
	pos := hash % n

	// Track current distance
	var d int64
	for {
		// Find offset of metric.
		offset := binary.BigEndian.Uint64(blk.hashData[MetricNSize+(pos*MetricOffsetSize):])
		if offset == 0 {
			return MetricBlockElem{}, false
		}

		// Evaluate name if offset is not empty.
		if offset > 0 {
			// Parse into element.
			var e MetricBlockElem
			e.UnmarshalBinary(blk.data[offset:])

			// Return if name match.
			if bytes.Equal(e.name, name) {
				return e, true
			}

			// Check if we've exceeded the probe distance.
			if d > rhh.Dist(rhh.HashKey(e.name), pos, n) {
				return MetricBlockElem{}, false
			}
		}

		// Move position forward.
		pos = (pos + 1) % n
		d++

		if d > n {
			return MetricBlockElem{}, false
		}
	}
}

// UnmarshalBinary unpacks data into the block. Block is not copied so data
// should be retained and unchanged after being passed into this function.
func (blk *MetricBlock) UnmarshalBinary(data []byte) error {
	// Read trailer.
	t, err := ReadMetricBlockTrailer(data)
	if err != nil {
		return err
	}

	// Save data section.
	blk.data = data[t.Data.Offset:]
	blk.data = blk.data[:t.Data.Size]

	// Save hash index block.
	blk.hashData = data[t.HashIndex.Offset:]
	blk.hashData = blk.hashData[:t.HashIndex.Size]

	// Initialise sketch data.
	blk.sketchData = data[t.Sketch.Offset:][:t.Sketch.Size]
	blk.tSketchData = data[t.TSketch.Offset:][:t.TSketch.Size]

	return nil
}

// Iterator returns an iterator over all metrics.
func (blk *MetricBlock) Iterator() MetricIterator {
	return &blockMetricIterator{data: blk.data[MetricFillSize:]}
}

// SeriesIDIterator returns an iterator for all series ids in a metric.
func (blk *MetricBlock) SeriesIDIterator(name []byte) tsdb.SeriesIDIterator {
	// Find metric element.
	e, ok := blk.Elem(name)
	if !ok {
		return &rawSeriesIDIterator{}
	}
	if e.seriesIDSet != nil {
		return tsdb.NewSeriesIDSetIterator(e.seriesIDSet)
	}
	return &rawSeriesIDIterator{n: e.series.n, data: e.series.data}
}

// Sketches returns existence and tombstone metric sketches.
func (blk *MetricBlock) Sketches() (sketch, tSketch estimator.Sketch, err error) {
	sketch = hll.NewDefaultPlus()
	if err := sketch.UnmarshalBinary(blk.sketchData); err != nil {
		return nil, nil, err
	}

	tSketch = hll.NewDefaultPlus()
	if err := tSketch.UnmarshalBinary(blk.tSketchData); err != nil {
		return nil, nil, err
	}
	return sketch, tSketch, nil
}

// blockMetricIterator iterates over a list metrics in a block.
type blockMetricIterator struct {
	elem MetricBlockElem
	data []byte
}

// Next returns the next metric. Returns nil when iterator is complete.
func (itr *blockMetricIterator) Next() MetricElem {
	// Return nil when we run out of data.
	if len(itr.data) == 0 {
		return nil
	}

	// Unmarshal the element at the current position.
	itr.elem.UnmarshalBinary(itr.data)

	// Move the data forward past the record.
	itr.data = itr.data[itr.elem.size:]

	return &itr.elem
}

// rawSeriesIterator iterates over a list of raw series data.
type rawSeriesIDIterator struct {
	prev uint64
	n    uint64
	data []byte
}

func (itr *rawSeriesIDIterator) Close() error { return nil }

// Next returns the next decoded series.
func (itr *rawSeriesIDIterator) Next() (tsdb.SeriesIDElem, error) {
	if len(itr.data) == 0 {
		return tsdb.SeriesIDElem{}, nil
	}

	delta, n, err := uvarint(itr.data)
	if err != nil {
		return tsdb.SeriesIDElem{}, err
	}
	itr.data = itr.data[n:]

	seriesID := itr.prev + uint64(delta)
	itr.prev = seriesID
	return tsdb.SeriesIDElem{SeriesID: seriesID}, nil
}

func (itr *rawSeriesIDIterator) SeriesIDSet() *tsdb.SeriesIDSet {
	ss := tsdb.NewSeriesIDSet()
	for data, prev := itr.data, uint64(0); len(data) > 0; {
		delta, n, err := uvarint(data)
		if err != nil {
			break
		}
		data = data[n:]

		seriesID := prev + uint64(delta)
		prev = seriesID
		ss.AddNoLock(seriesID)
	}
	return ss
}

// MetricBlockTrailer represents meta data at the end of a MetricBlock.
type MetricBlockTrailer struct {
	Version int // Encoding version

	// Offset & size of data section.
	Data struct {
		Offset int64
		Size   int64
	}

	// Offset & size of hash map section.
	HashIndex struct {
		Offset int64
		Size   int64
	}

	// Offset and size of cardinality sketch for metrics.
	Sketch struct {
		Offset int64
		Size   int64
	}

	// Offset and size of cardinality sketch for tombstoned metrics.
	TSketch struct {
		Offset int64
		Size   int64
	}
}

// ReadMetricBlockTrailer returns the block trailer from data.
func ReadMetricBlockTrailer(data []byte) (MetricBlockTrailer, error) {
	var t MetricBlockTrailer

	// Read version (which is located in the last two bytes of the trailer).
	t.Version = int(binary.BigEndian.Uint16(data[len(data)-2:]))
	if t.Version != MetricBlockVersion {
		return t, ErrUnsupportedIndexFileVersion
	}

	// Slice trailer data.
	buf := data[len(data)-MetricTrailerSize:]

	// Read data section info.
	t.Data.Offset, buf = int64(binary.BigEndian.Uint64(buf[0:8])), buf[8:]
	t.Data.Size, buf = int64(binary.BigEndian.Uint64(buf[0:8])), buf[8:]

	// Read metric block info.
	t.HashIndex.Offset, buf = int64(binary.BigEndian.Uint64(buf[0:8])), buf[8:]
	t.HashIndex.Size, buf = int64(binary.BigEndian.Uint64(buf[0:8])), buf[8:]

	// Read metric sketch info.
	t.Sketch.Offset, buf = int64(binary.BigEndian.Uint64(buf[0:8])), buf[8:]
	t.Sketch.Size, buf = int64(binary.BigEndian.Uint64(buf[0:8])), buf[8:]

	// Read tombstone metric sketch info.
	t.TSketch.Offset, buf = int64(binary.BigEndian.Uint64(buf[0:8])), buf[8:]
	t.TSketch.Size = int64(binary.BigEndian.Uint64(buf[0:8]))

	return t, nil
}

// WriteTo writes the trailer to w.
func (t *MetricBlockTrailer) WriteTo(w io.Writer) (n int64, err error) {
	// Write data section info.
	if err := writeUint64To(w, uint64(t.Data.Offset), &n); err != nil {
		return n, err
	} else if err := writeUint64To(w, uint64(t.Data.Size), &n); err != nil {
		return n, err
	}

	// Write hash index section info.
	if err := writeUint64To(w, uint64(t.HashIndex.Offset), &n); err != nil {
		return n, err
	} else if err := writeUint64To(w, uint64(t.HashIndex.Size), &n); err != nil {
		return n, err
	}

	// Write metric sketch info.
	if err := writeUint64To(w, uint64(t.Sketch.Offset), &n); err != nil {
		return n, err
	} else if err := writeUint64To(w, uint64(t.Sketch.Size), &n); err != nil {
		return n, err
	}

	// Write tombstone metric sketch info.
	if err := writeUint64To(w, uint64(t.TSketch.Offset), &n); err != nil {
		return n, err
	} else if err := writeUint64To(w, uint64(t.TSketch.Size), &n); err != nil {
		return n, err
	}

	// Write metric block version.
	if err := writeUint16To(w, MetricBlockVersion, &n); err != nil {
		return n, err
	}

	return n, nil
}

// MetricBlockElem represents an internal metric element.
type MetricBlockElem struct {
	flag byte   // flag
	name []byte // metric name

	tagBlock struct {
		offset int64
		size   int64
	}

	series struct {
		n    uint64 // series count
		data []byte // serialized series data
	}

	seriesIDSet *tsdb.SeriesIDSet

	// size in bytes, set after unmarshaling.
	size int
}

// Name returns the metric name.
func (e *MetricBlockElem) Name() []byte { return e.name }

// Deleted returns true if the tombstone flag is set.
func (e *MetricBlockElem) Deleted() bool {
	return (e.flag & MetricTombstoneFlag) != 0
}

// TagBlockOffset returns the offset of the metric's tag block.
func (e *MetricBlockElem) TagBlockOffset() int64 { return e.tagBlock.offset }

// TagBlockSize returns the size of the metric's tag block.
func (e *MetricBlockElem) TagBlockSize() int64 { return e.tagBlock.size }

// SeriesData returns the raw series data.
func (e *MetricBlockElem) SeriesData() []byte { return e.series.data }

// SeriesN returns the number of series associated with the metric.
func (e *MetricBlockElem) SeriesN() uint64 { return e.series.n }

// SeriesID returns series ID at an index.
func (e *MetricBlockElem) SeriesID(i int) uint64 {
	return binary.BigEndian.Uint64(e.series.data[i*SeriesIDSize:])
}

func (e *MetricBlockElem) HasSeries() bool { return e.series.n > 0 }

// SeriesIDs returns a list of decoded series ids.
//
// NOTE: This should be used for testing and diagnostics purposes only.
// It requires loading the entire list of series in-memory.
func (e *MetricBlockElem) SeriesIDs() []uint64 {
	a := make([]uint64, 0, e.series.n)
	e.ForEachSeriesID(func(id uint64) error {
		a = append(a, id)
		return nil
	})
	return a
}

func (e *MetricBlockElem) ForEachSeriesID(fn func(uint64) error) error {
	// Read from roaring, if available.
	if e.seriesIDSet != nil {
		itr := e.seriesIDSet.Iterator()
		for itr.HasNext() {
			if err := fn(uint64(itr.Next())); err != nil {
				return err
			}
		}
	}

	// Read from uvarint encoded data, if available.
	var prev uint64
	for data := e.series.data; len(data) > 0; {
		delta, n, err := uvarint(data)
		if err != nil {
			return err
		}
		data = data[n:]

		seriesID := prev + uint64(delta)
		if err = fn(seriesID); err != nil {
			return err
		}
		prev = seriesID
	}
	return nil
}

// Size returns the size of the element.
func (e *MetricBlockElem) Size() int { return e.size }

// UnmarshalBinary unmarshals data into e.
func (e *MetricBlockElem) UnmarshalBinary(data []byte) error {
	start := len(data)

	// Parse flag data.
	e.flag, data = data[0], data[1:]

	// Parse tag block offset.
	e.tagBlock.offset, data = int64(binary.BigEndian.Uint64(data)), data[8:]
	e.tagBlock.size, data = int64(binary.BigEndian.Uint64(data)), data[8:]

	// Parse name.
	sz, n, err := uvarint(data)
	if err != nil {
		return err
	}
	e.name, data = data[n:n+int(sz)], data[n+int(sz):]

	// Parse series count.
	v, n, err := uvarint(data)
	if err != nil {
		return err
	}
	e.series.n, data = uint64(v), data[n:]

	// Parse series data size.
	sz, n, err = uvarint(data)
	if err != nil {
		return err
	}
	data = data[n:]

	// Parse series data (original uvarint encoded or roaring bitmap).
	if e.flag&MetricSeriesIDSetFlag == 0 {
		e.series.data, data = data[:sz], data[sz:]
	} else {
		// data = memalign(data)
		e.seriesIDSet = tsdb.NewSeriesIDSet()
		if err = e.seriesIDSet.UnmarshalBinaryUnsafe(data[:sz]); err != nil {
			return err
		}
		data = data[sz:]
	}

	// Save length of elem.
	e.size = start - len(data)

	return nil
}

// MetricBlockWriter writes a metric block.
type MetricBlockWriter struct {
	buf bytes.Buffer
	mms map[string]metric

	// Metric sketch and tombstoned metric sketch.
	sketch, tSketch estimator.Sketch
}

// NewMetricBlockWriter returns a new MetricBlockWriter.
func NewMetricBlockWriter() *MetricBlockWriter {
	return &MetricBlockWriter{
		mms:     make(map[string]metric),
		sketch:  hll.NewDefaultPlus(),
		tSketch: hll.NewDefaultPlus(),
	}
}

// Add adds a metric with series and tag set offset/size.
func (mw *MetricBlockWriter) Add(name []byte, deleted bool, offset, size int64, seriesIDs []uint64) {
	mm := mw.mms[string(name)]
	mm.deleted = deleted
	mm.tagBlock.offset = offset
	mm.tagBlock.size = size

	if mm.seriesIDSet == nil {
		mm.seriesIDSet = tsdb.NewSeriesIDSet()
	}
	for _, seriesID := range seriesIDs {
		mm.seriesIDSet.AddNoLock(seriesID)
	}

	mw.mms[string(name)] = mm

	if deleted {
		mw.tSketch.Add(name)
	} else {
		mw.sketch.Add(name)
	}
}

// WriteTo encodes the metrics to w.
func (mw *MetricBlockWriter) WriteTo(w io.Writer) (n int64, err error) {
	var t MetricBlockTrailer

	// The sketches must be set before calling WriteTo.
	if mw.sketch == nil {
		return 0, errors.New("metric sketch not set")
	} else if mw.tSketch == nil {
		return 0, errors.New("metric tombstone sketch not set")
	}

	// Sort names.
	names := make([]string, 0, len(mw.mms))
	for name := range mw.mms {
		names = append(names, name)
	}
	sort.Strings(names)

	// Begin data section.
	t.Data.Offset = n

	// Write padding byte so no offsets are zero.
	if err := writeUint8To(w, 0, &n); err != nil {
		return n, err
	}

	// Encode key list.
	for _, name := range names {
		// Retrieve metric and save offset.
		mm := mw.mms[name]
		mm.offset = n
		mw.mms[name] = mm

		// Write metric
		if err := mw.writeMetricTo(w, []byte(name), &mm, &n); err != nil {
			return n, err
		}
	}
	t.Data.Size = n - t.Data.Offset

	// Build key hash map
	m := rhh.NewHashMap(rhh.Options{
		Capacity:   int64(len(names)),
		LoadFactor: LoadFactor,
	})
	for name := range mw.mms {
		mm := mw.mms[name]
		m.Put([]byte(name), &mm)
	}

	t.HashIndex.Offset = n

	// Encode hash map length.
	if err := writeUint64To(w, uint64(m.Cap()), &n); err != nil {
		return n, err
	}

	// Encode hash map offset entries.
	for i := int64(0); i < m.Cap(); i++ {
		_, v := m.Elem(i)

		var offset int64
		if mm, ok := v.(*metric); ok {
			offset = mm.offset
		}

		if err := writeUint64To(w, uint64(offset), &n); err != nil {
			return n, err
		}
	}
	t.HashIndex.Size = n - t.HashIndex.Offset

	// Write the sketches out.
	t.Sketch.Offset = n
	if err := writeSketchTo(w, mw.sketch, &n); err != nil {
		return n, err
	}
	t.Sketch.Size = n - t.Sketch.Offset

	t.TSketch.Offset = n
	if err := writeSketchTo(w, mw.tSketch, &n); err != nil {
		return n, err
	}
	t.TSketch.Size = n - t.TSketch.Offset

	// Write trailer.
	nn, err := t.WriteTo(w)
	n += nn
	return n, err
}

// writeMetricTo encodes a single metric entry into w.
func (mw *MetricBlockWriter) writeMetricTo(w io.Writer, name []byte, mm *metric, n *int64) error {
	// Write flag & tag block offset.
	if err := writeUint8To(w, mm.flag(), n); err != nil {
		return err
	}
	if err := writeUint64To(w, uint64(mm.tagBlock.offset), n); err != nil {
		return err
	} else if err := writeUint64To(w, uint64(mm.tagBlock.size), n); err != nil {
		return err
	}

	// Write metric name.
	if err := writeUvarintTo(w, uint64(len(name)), n); err != nil {
		return err
	}
	if err := writeTo(w, name, n); err != nil {
		return err
	}

	// Write series data to buffer.
	mw.buf.Reset()
	if _, err := mm.seriesIDSet.WriteTo(&mw.buf); err != nil {
		return err
	}

	// Write series count.
	if err := writeUvarintTo(w, mm.seriesIDSet.Cardinality(), n); err != nil {
		return err
	}

	// Write data size & buffer.
	if err := writeUvarintTo(w, uint64(mw.buf.Len()), n); err != nil {
		return err
	}

	// Word align bitmap data.
	// if offset := (*n) % 8; offset != 0 {
	// 	if err := writeTo(w, make([]byte, 8-offset), n); err != nil {
	// 		return err
	// 	}
	// }

	nn, err := mw.buf.WriteTo(w)
	*n += nn
	return err
}

// writeSketchTo writes an estimator.Sketch into w, updating the number of bytes
// written via n.
func writeSketchTo(w io.Writer, s estimator.Sketch, n *int64) error {
	data, err := s.MarshalBinary()
	if err != nil {
		return err
	}

	nn, err := w.Write(data)
	*n += int64(nn)
	return err
}

type metric struct {
	deleted  bool
	tagBlock struct {
		offset int64
		size   int64
	}
	seriesIDSet *tsdb.SeriesIDSet
	offset      int64
}

func (mm metric) flag() byte {
	flag := byte(MetricSeriesIDSetFlag)
	if mm.deleted {
		flag |= MetricTombstoneFlag
	}
	return flag
}
