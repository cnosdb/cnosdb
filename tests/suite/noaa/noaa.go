package noaa

import (
	"bufio"
	"fmt"
	"github.com/cnosdb/cnosdb/tests"
	"net/url"
	"os"
	"testing"
	"time"
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
	i := 0
	tNow := time.Now()
	for scan.Scan() {
		i++
		if i%5000 == 0 {
			fmt.Printf("Rows: %d, Time Cost: %s\n", i, time.Now().Sub(tNow).String())
		}
		params := url.Values{"precision": []string{"s"}}
		n.S.MustWrite(db, rp, scan.Text(), params)
	}
	fmt.Printf("Rows: %d, Time Cost: %s\n", i, time.Now().Sub(tNow).String())
	if err = scan.Err(); err != nil {
		n.T.Error(err)
	}
}

func (n *NOAA) Test() {
	for i, c := range cases {
		n.T.Run(fmt.Sprintf("T%d-%s", i, c.Name), func(t *testing.T) {
			c.Run("NOAA", n.S, n.T)
		})
	}
}
