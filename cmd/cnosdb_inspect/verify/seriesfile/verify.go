package seriesfile

import (
	"fmt"
	"github.com/cnosdb/db/tsdb"
	"go.uber.org/zap"
	"sort"
)

// Verify contains configuration for running verification of series files.
type Verify struct {
	Concurrent int
	Logger     *zap.Logger

	done chan struct{}
}

func NewVerify(concurrent int) *Verify {
	return &Verify{
		Concurrent: concurrent,
		Logger:     zap.NewNop(),
	}
}

// verifyResult contains the result of a Verify___ call
type verifyResult struct {
	valid bool
	err   error
}

func (v Verify) VerifySeriesFile(filePath string) (valid bool, err error) {
	return false, nil
}

// VerifyPartition performs verifications on a partition of a series seriesFile. The error is only returned
// if there was some fatal problem with operating, not if there was a problem with the partition.
func (v Verify) VerifyPartition(partitionPath string) (valid bool, err error) {
	return false, nil
}

// IDData keeps track of data about a series ID.
type IDData struct {
	Offset  int64
	Key     []byte
	Deleted bool
}

// VerifySegment performs verifications on a segment of a series seriesFile. The error is only returned
// if there was some fatal problem with operating, not if there was a problem with the partition.
// The ids map is populated with information about the ids stored in the segment.
func (v Verify) VerifySegment(segmentPath string, ids map[uint64]IDData) (valid bool, err error) {
	return false, nil
}

// VerifyIndex performs verification on an index in a series seriesFile. The error is only returned
// if there was some fatal problem with operating, not if there was a problem with the partition.
// The ids map must be built from verifying the passed in segments.
func (v Verify) VerifyIndex(indexPath string, segments []*tsdb.SeriesSegment,
	ids map[uint64]IDData) (valid bool, err error) {
	v.Logger.Info("Verifying index")

	defer func() {
		if rec := recover(); rec != nil {
			v.Logger.Error("Panic verifying index", zap.String("recovered", fmt.Sprint(rec)))
			valid = false
		}
	}()

	index := tsdb.NewSeriesIndex(indexPath)
	if err := index.Open(); err != nil {
		v.Logger.Error("Error opening index", zap.Error(err))
		return false, nil
	}
	defer index.Close()

	if err := index.Recover(segments); err != nil {
		v.Logger.Error("Error recovering index", zap.Error(err))
		return false, nil
	}

	// we check all the ids in a consistent order to get the same errors if
	// there is a problem
	idsList := make([]uint64, 0, len(ids))
	for id := range ids {
		idsList = append(idsList, id)
	}
	sort.Slice(idsList, func(i, j int) bool {
		return idsList[i] < idsList[j]
	})

	for _, id := range idsList {
		select {
		default:
		case <-v.done:
			return false, nil
		}

		IDData := ids[id]

		expectedOffset, expectedID := IDData.Offset, id
		if IDData.Deleted {
			expectedOffset, expectedID = 0, 0
		}

		// check both that the offset is right and that we get the right
		// id for the key
		if gotOffset := index.FindOffsetByID(id); gotOffset != expectedOffset {
			v.Logger.Error("Index inconsistency",
				zap.Uint64("id", id),
				zap.Int64("got_offset", gotOffset),
				zap.Int64("expected_offset", expectedOffset))
			return false, nil
		}

		if gotID := index.FindIDBySeriesKey(segments, IDData.Key); gotID != expectedID {
			v.Logger.Error("Index inconsistency",
				zap.Uint64("id", id),
				zap.Uint64("got_id", gotID),
				zap.Uint64("expected_id", expectedID))
			return false, nil
		}
	}

	return true, nil

}

// buffer allows one to safely advance a byte slice and keep track of how many bytes were advanced.
type buffer struct {
	offset int64
	data   []byte
}

// newBuffer constructs a buffer with the provided data.
func newBuffer(data []byte) *buffer {
	return &buffer{
		offset: 0,
		data:   data,
	}
}

// advance will consume n bytes from the data slice and return an error if there is not enough
// data to do so.
func (b *buffer) advance(n int64) error {
	if int64(len(b.data)) < n {
		return fmt.Errorf("unable to advance %d bytes: %d remaining", n, len(b.data))
	}
	b.data = b.data[n:]
	b.offset += n
	return nil
}
