package tests

import (
	"flag"
	"fmt"
	"github.com/cnosdb/cnosdb/pkg/logger"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/cnosdb/cnosdb/vend/db/tsdb"
)

// Global server used by benchmarks
var benchServer Server

func TestMain(m *testing.M) {
	flag.BoolVar(&verboseServerLogs, "vv", false, "Turn on very verbose server logging.")
	flag.BoolVar(&cleanupData, "clean", true, "Clean up test data on disk.")
	flag.Int64Var(&seed, "seed", 0, "Set specific seed controlling randomness.")
	flag.Parse()

	// Set random seed if not explicitly set.
	if seed == 0 {
		seed = time.Now().UnixNano()
	}
	rand.Seed(seed)

	var r int
	for _, indexType = range tsdb.RegisteredIndexes() {
		//setup server
		c := NewConfig()
		c.RetentionPolicy.Enabled = false
		c.Monitor.StoreEnabled = false
		c.Subscriber.Enabled = false
		c.ContinuousQuery.Enabled = true
		c.Data.MaxValuesPerTag = 1000000 // 1M
		c.Data.Index = indexType
		c.Log = logger.NewDefaultLogConfig()
		if err := logger.InitZapLogger(c.Log); err != nil {
			fmt.Printf("parse log config: %s\n", err)
		}
		benchServer = OpenDefaultServer(c)



		if testing.Verbose() {
			fmt.Println("================ Running all tests for index ================")
		}

		if curr := m.Run(); r == 0 {
			r = curr
		}


		benchServer.Close()
		if testing.Verbose() {
			fmt.Println()
		}
	}
	os.Exit(r)
}


