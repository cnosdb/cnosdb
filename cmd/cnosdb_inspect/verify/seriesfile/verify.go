package seriesfile

import (
	"fmt"
	"github.com/cnosdb/db/tsdb"
	"go.uber.org/zap"
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
	return false, nil
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
