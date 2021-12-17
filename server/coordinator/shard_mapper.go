package coordinator

import (
	"context"
	"io"
	"net"
	"time"

	"github.com/cnosdatabase/cnosdb/meta"
	"github.com/cnosdatabase/cnosdb/pkg/network"
	"github.com/cnosdatabase/cnosql"
	"github.com/cnosdatabase/db/query"
	"github.com/cnosdatabase/db/tsdb"
)

// IteratorCreator is an interface that combines mapping fields and creating iterators.
type IteratorCreator interface {
	query.IteratorCreator
	cnosql.FieldMapper
	io.Closer
}

// LocalShardMapper implements a ShardMapper for local shards.
type LocalShardMapper struct {
	MetaClient interface {
		ShardGroupsByTimeRange(database, rp string, min, max time.Time) (a []meta.ShardGroupInfo, err error)
	}

	TSDBStore interface {
		ShardGroup(ids []uint64) tsdb.ShardGroup
	}
}

// MapShards maps the sources to the appropriate shards into an IteratorCreator.
func (e *LocalShardMapper) MapShards(sources cnosql.Sources, t cnosql.TimeRange, opt query.SelectOptions) (query.ShardGroup, error) {
	a := &LocalShardMapping{
		ShardMap: make(map[Source]tsdb.ShardGroup),
	}

	tmin := time.Unix(0, t.MinTimeNano())
	tmax := time.Unix(0, t.MaxTimeNano())
	if err := e.mapShards(a, sources, tmin, tmax); err != nil {
		return nil, err
	}
	a.MinTime, a.MaxTime = tmin, tmax
	return a, nil
}

func (e *LocalShardMapper) mapShards(a *LocalShardMapping, sources cnosql.Sources, tmin, tmax time.Time) error {
	for _, s := range sources {
		switch s := s.(type) {
		case *cnosql.Measurement:
			source := Source{
				Database:        s.Database,
				RetentionPolicy: s.RetentionPolicy,
			}
			// Retrieve the list of shards for this database. This list of
			// shards is always the same regardless of which measurement we are
			// using.
			if _, ok := a.ShardMap[source]; !ok {
				groups, err := e.MetaClient.ShardGroupsByTimeRange(s.Database, s.RetentionPolicy, tmin, tmax)
				if err != nil {
					return err
				}

				if len(groups) == 0 {
					a.ShardMap[source] = nil
					continue
				}

				shardIDs := make([]uint64, 0, len(groups[0].Shards)*len(groups))
				for _, g := range groups {
					for _, si := range g.Shards {
						shardIDs = append(shardIDs, si.ID)
					}
				}
				a.ShardMap[source] = e.TSDBStore.ShardGroup(shardIDs)
			}
		case *cnosql.SubQuery:
			if err := e.mapShards(a, s.Statement.Sources, tmin, tmax); err != nil {
				return err
			}
		}
	}
	return nil
}

// LocalShardMapping maps data sources to a list of shard information.
type LocalShardMapping struct {
	ShardMap map[Source]tsdb.ShardGroup

	// MinTime is the minimum time that this shard mapper will allow.
	// Any attempt to use a time before this one will automatically result in using
	// this time instead.
	MinTime time.Time

	// MaxTime is the maximum time that this shard mapper will allow.
	// Any attempt to use a time after this one will automatically result in using
	// this time instead.
	MaxTime time.Time
}

func (a *LocalShardMapping) FieldDimensions(m *cnosql.Measurement) (fields map[string]cnosql.DataType, dimensions map[string]struct{}, err error) {
	source := Source{
		Database:        m.Database,
		RetentionPolicy: m.RetentionPolicy,
	}

	rg := a.ShardMap[source]
	if rg == nil {
		return
	}

	fields = make(map[string]cnosql.DataType)
	dimensions = make(map[string]struct{})

	var measurements []string
	if m.Regex != nil {
		measurements = rg.MeasurementsByRegex(m.Regex.Val)
	} else {
		measurements = []string{m.Name}
	}

	f, d, err := rg.FieldDimensions(measurements)
	if err != nil {
		return nil, nil, err
	}
	for k, typ := range f {
		fields[k] = typ
	}
	for k := range d {
		dimensions[k] = struct{}{}
	}
	return
}

func (a *LocalShardMapping) MapType(m *cnosql.Measurement, field string) cnosql.DataType {
	source := Source{
		Database:        m.Database,
		RetentionPolicy: m.RetentionPolicy,
	}

	rg := a.ShardMap[source]
	if rg == nil {
		return cnosql.Unknown
	}

	var names []string
	if m.Regex != nil {
		names = rg.MeasurementsByRegex(m.Regex.Val)
	} else {
		names = []string{m.Name}
	}

	var typ cnosql.DataType
	for _, name := range names {
		if m.SystemIterator != "" {
			name = m.SystemIterator
		}
		t := rg.MapType(name, field)
		if typ.LessThan(t) {
			typ = t
		}
	}
	return typ
}

func (a *LocalShardMapping) CreateIterator(ctx context.Context, m *cnosql.Measurement, opt query.IteratorOptions) (query.Iterator, error) {
	source := Source{
		Database:        m.Database,
		RetentionPolicy: m.RetentionPolicy,
	}

	rg := a.ShardMap[source]
	if rg == nil {
		return nil, nil
	}

	// Override the time constraints if they don't match each other.
	if !a.MinTime.IsZero() && opt.StartTime < a.MinTime.UnixNano() {
		opt.StartTime = a.MinTime.UnixNano()
	}
	if !a.MaxTime.IsZero() && opt.EndTime > a.MaxTime.UnixNano() {
		opt.EndTime = a.MaxTime.UnixNano()
	}

	if m.Regex != nil {
		measurements := rg.MeasurementsByRegex(m.Regex.Val)
		inputs := make([]query.Iterator, 0, len(measurements))
		if err := func() error {
			// Create a Measurement for each returned matching measurement value
			// from the regex.
			for _, measurement := range measurements {
				mm := m.Clone()
				mm.Name = measurement // Set the name to this matching regex value.
				input, err := rg.CreateIterator(ctx, mm, opt)
				if err != nil {
					return err
				}
				inputs = append(inputs, input)
			}
			return nil
		}(); err != nil {
			query.Iterators(inputs).Close()
			return nil, err
		}

		return query.Iterators(inputs).Merge(opt)
	}
	return rg.CreateIterator(ctx, m, opt)
}

func (a *LocalShardMapping) IteratorCost(m *cnosql.Measurement, opt query.IteratorOptions) (query.IteratorCost, error) {
	source := Source{
		Database:        m.Database,
		RetentionPolicy: m.RetentionPolicy,
	}

	rg := a.ShardMap[source]
	if rg == nil {
		return query.IteratorCost{}, nil
	}

	// Override the time constraints if they don't match each other.
	if !a.MinTime.IsZero() && opt.StartTime < a.MinTime.UnixNano() {
		opt.StartTime = a.MinTime.UnixNano()
	}
	if !a.MaxTime.IsZero() && opt.EndTime > a.MaxTime.UnixNano() {
		opt.EndTime = a.MaxTime.UnixNano()
	}

	if m.Regex != nil {
		var costs query.IteratorCost
		measurements := rg.MeasurementsByRegex(m.Regex.Val)
		for _, measurement := range measurements {
			cost, err := rg.IteratorCost(measurement, opt)
			if err != nil {
				return query.IteratorCost{}, err
			}
			costs = costs.Combine(cost)
		}
		return costs, nil
	}
	return rg.IteratorCost(m.Name, opt)
}

// Close clears out the list of mapped shards.
func (a *LocalShardMapping) Close() error {
	a.ShardMap = nil
	return nil
}

// remoteIteratorCreator creates iterators for remote shards.
type remoteIteratorCreator struct {
	dialer   *NodeDialer
	nodeID   uint64
	shardIDs []uint64
}

// newRemoteIteratorCreator returns a new instance of remoteIteratorCreator for a remote shard.
func newRemoteIteratorCreator(dialer *NodeDialer, nodeID uint64, shardIDs []uint64) *remoteIteratorCreator {
	return &remoteIteratorCreator{
		dialer:   dialer,
		nodeID:   nodeID,
		shardIDs: shardIDs,
	}
}

// CreateIterator creates a remote streaming iterator.
func (ic *remoteIteratorCreator) CreateIterator(ctx context.Context, m *cnosql.Measurement, opt query.IteratorOptions) (query.Iterator, error) {
	conn, err := ic.dialer.DialNode(ic.nodeID)
	if err != nil {
		return nil, err
	}

	var resp CreateIteratorResponse
	if err := func() error {
		// Write request.
		if err := EncodeTLV(conn, createIteratorRequestMessage, &CreateIteratorRequest{
			ShardIDs: ic.shardIDs,
			Opt:      opt,
		}); err != nil {
			return err
		}

		// Read the response.
		if _, err := DecodeTLV(conn, &resp); err != nil {
			return err
		} else if resp.Err != nil {
			return err
		}

		return nil
	}(); err != nil {
		conn.Close()
		return nil, err
	}

	return query.NewReaderIterator(ctx, conn, resp.typ, resp.stats), nil
}

// FieldDimensions returns the unique fields and dimensions across a list of sources.
func (ic *remoteIteratorCreator) FieldDimensions(km *cnosql.Measurement) (fields map[string]cnosql.DataType, dimensions map[string]struct{}, err error) {
	conn, err := ic.dialer.DialNode(ic.nodeID)
	if err != nil {
		return nil, nil, err
	}
	defer conn.Close()

	// Write request.
	if err := EncodeTLV(conn, fieldDimensionsRequestMessage, &FieldDimensionsRequest{
		ShardIDs:    ic.shardIDs,
		Measurement: *km,
	}); err != nil {
		return nil, nil, err
	}

	// Read the response.
	var resp FieldDimensionsResponse
	if _, err := DecodeTLV(conn, &resp); err != nil {
		return nil, nil, err
	}
	return resp.Fields, resp.Dimensions, resp.Err
}

// NodeDialer dials connections to a given node.
type NodeDialer struct {
	MetaClient MetaClient
	Timeout    time.Duration
}

// DialNode returns a connection to a node.
func (d *NodeDialer) DialNode(nodeID uint64) (net.Conn, error) {
	ni, err := d.MetaClient.DataNode(nodeID)
	if err != nil {
		return nil, err
	}

	conn, err := network.DialTimeout("tcp", ni.TCPHost, MuxHeader, d.Timeout)
	if err != nil {
		return nil, err
	}
	conn.SetDeadline(time.Now().Add(d.Timeout))

	return conn, nil
}

// Source contains the database and time to live source for data.
type Source struct {
	Database        string
	RetentionPolicy string
}
