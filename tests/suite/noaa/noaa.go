package noaa

import (
	"bufio"
	"github.com/cnosdb/cnosdb/tests"
	"os"
	"testing"
)

const (
	dataFile = "../noaa/NOAA_data.txt"
	db       = "NOAA_water_database"
	rp       = "rp0"
)

type NOAA struct {
	T *testing.T
	S tests.Server
}

func (n *NOAA) Load() {
	f, err := os.Open(dataFile)
	if err != nil {
		n.T.Error(err)
	}
	defer func(f *os.File) {
		if e := f.Close(); e != nil {
			n.T.Error(err)
		}
	}(f)

	rp0 := tests.NewRetentionPolicySpec(rp, 1, 0)
	if err = n.S.CreateDatabaseAndRetentionPolicy(db, rp0, true); err != nil {
		n.T.Error(err)
	}

	scan := bufio.NewScanner(f)
	for scan.Scan() {
		n.S.MustWrite(db, rp, scan.Text(), nil)
	}
	if err = scan.Err(); err != nil {
		n.T.Error(err)
	}
}
