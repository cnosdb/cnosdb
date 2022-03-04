package seriesfile

import (
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
