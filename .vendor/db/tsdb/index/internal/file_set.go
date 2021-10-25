package internal

import (
	"github.com/cnosdatabase/db/models"
	"github.com/cnosdatabase/db/pkg/bloom"
	"github.com/cnosdatabase/db/pkg/estimator"
	"github.com/cnosdatabase/db/tsdb"
	"github.com/cnosdatabase/db/tsdb/index/tsi1"
)

// File is a mock implementation of a tsi1.File.
type File struct {
	Closef                    func() error
	Pathf                     func() string
	IDf                       func() int
	Levelf                    func() int
	Metricf                   func(name []byte) tsi1.MetricElem
	MetricIteratorf           func() tsi1.MetricIterator
	HasSeriesf                func(name []byte, tags models.Tags, buf []byte) (exists, tombstoned bool)
	TagKeyf                   func(name, key []byte) tsi1.TagKeyElem
	TagKeyIteratorf           func(name []byte) tsi1.TagKeyIterator
	TagValuef                 func(name, key, value []byte) tsi1.TagValueElem
	TagValueIteratorf         func(name, key []byte) tsi1.TagValueIterator
	SeriesIDIteratorf         func() tsdb.SeriesIDIterator
	MetricSeriesIDIteratorf   func(name []byte) tsdb.SeriesIDIterator
	TagKeySeriesIDIteratorf   func(name, key []byte) tsdb.SeriesIDIterator
	TagValueSeriesIDIteratorf func(name, key, value []byte) tsdb.SeriesIDIterator
	MergeSeriesSketchesf      func(s, t estimator.Sketch) error
	MergeMetricsSketchesf     func(s, t estimator.Sketch) error
	Retainf                   func()
	Releasef                  func()
	Filterf                   func() *bloom.Filter
}

func (f *File) Close() error                        { return f.Closef() }
func (f *File) Path() string                        { return f.Pathf() }
func (f *File) ID() int                             { return f.IDf() }
func (f *File) Level() int                          { return f.Levelf() }
func (f *File) Metric(name []byte) tsi1.MetricElem  { return f.Metricf(name) }
func (f *File) MetricIterator() tsi1.MetricIterator { return f.MetricIteratorf() }
func (f *File) HasSeries(name []byte, tags models.Tags, buf []byte) (exists, tombstoned bool) {
	return f.HasSeriesf(name, tags, buf)
}
func (f *File) TagKey(name, key []byte) tsi1.TagKeyElem        { return f.TagKeyf(name, key) }
func (f *File) TagKeyIterator(name []byte) tsi1.TagKeyIterator { return f.TagKeyIteratorf(name) }

func (f *File) TagValue(name, key, value []byte) tsi1.TagValueElem {
	return f.TagValuef(name, key, value)
}
func (f *File) TagValueIterator(name, key []byte) tsi1.TagValueIterator {
	return f.TagValueIteratorf(name, key)
}
func (f *File) SeriesIDIterator() tsdb.SeriesIDIterator { return f.SeriesIDIteratorf() }
func (f *File) MetricSeriesIDIterator(name []byte) tsdb.SeriesIDIterator {
	return f.MetricSeriesIDIteratorf(name)
}
func (f *File) TagKeySeriesIDIterator(name, key []byte) tsdb.SeriesIDIterator {
	return f.TagKeySeriesIDIteratorf(name, key)
}
func (f *File) TagValueSeriesIDIterator(name, key, value []byte) tsdb.SeriesIDIterator {
	return f.TagValueSeriesIDIteratorf(name, key, value)
}
func (f *File) MergeSeriesSketches(s, t estimator.Sketch) error { return f.MergeSeriesSketchesf(s, t) }
func (f *File) MergeMetricsSketches(s, t estimator.Sketch) error {
	return f.MergeMetricsSketchesf(s, t)
}
func (f *File) Retain()               { f.Retainf() }
func (f *File) Release()              { f.Releasef() }
func (f *File) Filter() *bloom.Filter { return f.Filterf() }
