package tests

import (
	"flag"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/cnosdb/cnosdb/pkg/logger"
	"github.com/cnosdb/cnosdb/vend/db/tsdb"
	"go.uber.org/zap/zapcore"
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
		c.Log.Level = zapcore.ErrorLevel
		if err := logger.InitZapLogger(c.Log); err != nil {
			fmt.Printf("parse log config: %s\n", err)
		}
		benchServer = OpenDefaultServer(c)

		if testing.Verbose() {
			fmt.Printf("================ Running all tests #{%v} index ================\n", indexType)
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

func TestServer_DatabaseCommands(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig())
	defer s.Close()

	test := tests.load(t, "database_commands")

	for _, query := range test.queries {
		t.Run(query.name, func(t *testing.T) {
			if query.skip {
				t.Skipf("SKIP:: %s", query.name)
			}
			if err := query.Execute(s); err != nil {
				t.Error(query.Error(err))
			} else if !query.success() {
				t.Error(query.failureMessage())
			}
		})
	}
}

func TestServer_Query_DropAndRecreateDatabase(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig())
	defer s.Close()

	test := tests.load(t, "drop_and_recreate_database")

	if err := s.CreateDatabaseAndRetentionPolicy(test.database(), NewRetentionPolicySpec(test.retentionPolicy(), 1, 0), true); err != nil {
		t.Fatal(err)
	}

	for i, query := range test.queries {
		t.Run(query.name, func(t *testing.T) {
			if i == 0 {
				if err := test.init(s); err != nil {
					t.Fatalf("test init failed: %s", err)
				}
			}
			if query.skip {
				t.Skipf("SKIP:: %s", query.name)
			}
			if err := query.Execute(s); err != nil {
				t.Error(query.Error(err))
			} else if !query.success() {
				t.Error(query.failureMessage())
			}
		})
	}
}

func TestServer_Query_DropDatabaseIsolated(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig())
	defer s.Close()

	test := tests.load(t, "drop_database_isolated")

	if err := s.CreateDatabaseAndRetentionPolicy(test.database(), NewRetentionPolicySpec(test.retentionPolicy(), 1, 0), true); err != nil {
		t.Fatal(err)
	}
	if err := s.CreateDatabaseAndRetentionPolicy("db1", NewRetentionPolicySpec("rp1", 1, 0), true); err != nil {
		t.Fatal(err)
	}

	for i, query := range test.queries {
		t.Run(query.name, func(t *testing.T) {
			if i == 0 {
				if err := test.init(s); err != nil {
					t.Fatalf("test init failed: %s", err)
				}
			}
			if query.skip {
				t.Skipf("SKIP:: %s", query.name)
			}
			if err := query.Execute(s); err != nil {
				t.Error(query.Error(err))
			} else if !query.success() {
				t.Error(query.failureMessage())
			}
		})
	}
}

func TestServer_Query_DeleteSeries(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig())
	defer s.Close()

	test := tests.load(t, "delete_series_time")

	if err := s.CreateDatabaseAndRetentionPolicy(test.database(), NewRetentionPolicySpec(test.retentionPolicy(), 1, 0), true); err != nil {
		t.Fatal(err)
	}

	for i, query := range test.queries {
		if i == 0 {
			if err := test.init(s); err != nil {
				t.Fatalf("test init failed: %s", err)
			}
		}
		if query.skip {
			t.Logf("SKIP:: %s", query.name)
			continue
		}
		if err := query.Execute(s); err != nil {
			t.Error(query.Error(err))
		} else if !query.success() {
			t.Error(query.failureMessage())
		}
	}
}

func TestServer_Query_DeleteSeries_TagFilter(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig())
	defer s.Close()

	test := tests.load(t, "delete_series_time_tag_filter")

	if err := s.CreateDatabaseAndRetentionPolicy(test.database(), NewRetentionPolicySpec(test.retentionPolicy(), 1, 0), true); err != nil {
		t.Fatal(err)
	}

	for i, query := range test.queries {
		t.Run(query.name, func(t *testing.T) {
			if i == 0 {
				if err := test.init(s); err != nil {
					t.Fatalf("test init failed: %s", err)
				}
			}
			if query.skip {
				t.Skipf("SKIP:: %s", query.name)
			}
			if err := query.Execute(s); err != nil {
				t.Error(query.Error(err))
			} else if !query.success() {
				t.Error(query.failureMessage())
			}
		})
	}
}

func TestServer_Query_DropAndRecreateSeries(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig())
	defer s.Close()

	test := tests.load(t, "drop_and_recreate_series")

	if err := s.CreateDatabaseAndRetentionPolicy(test.database(), NewRetentionPolicySpec(test.retentionPolicy(), 1, 0), true); err != nil {
		t.Fatal(err)
	}

	for i, query := range test.queries {
		t.Run(query.name, func(t *testing.T) {
			if i == 0 {
				if err := test.init(s); err != nil {
					t.Fatalf("test init failed: %s", err)
				}
			}
			if query.skip {
				t.Skipf("SKIP:: %s", query.name)
			}
			if err := query.Execute(s); err != nil {
				t.Error(query.Error(err))
			} else if !query.success() {
				t.Error(query.failureMessage())
			}
		})
	}

	// Re-write data and test again.
	retest := tests.load(t, "drop_and_recreate_series_retest")

	for i, query := range retest.queries {
		t.Run(query.name, func(t *testing.T) {
			if i == 0 {
				if err := retest.init(s); err != nil {
					t.Fatalf("test init failed: %s", err)
				}
			}
			if query.skip {
				t.Skipf("SKIP:: %s", query.name)
			}
			if err := query.Execute(s); err != nil {
				t.Error(query.Error(err))
			} else if !query.success() {
				t.Error(query.failureMessage())
			}
		})
	}
}

func TestServer_Query_DropSeriesFromRegex(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig())
	defer s.Close()

	test := tests.load(t, "drop_series_from_regex")

	if err := s.CreateDatabaseAndRetentionPolicy(test.database(), NewRetentionPolicySpec(test.retentionPolicy(), 1, 0), true); err != nil {
		t.Fatal(err)
	}

	for i, query := range test.queries {
		t.Run(query.name, func(t *testing.T) {
			if i == 0 {
				if err := test.init(s); err != nil {
					t.Fatalf("test init failed: %s", err)
				}
			}
			if query.skip {
				t.Skipf("SKIP:: %s", query.name)
			}
			if err := query.Execute(s); err != nil {
				t.Error(query.Error(err))
			} else if !query.success() {
				t.Error(query.failureMessage())
			}
		})
	}
}

func TestServer_RetentionPolicyCommands(t *testing.T) {
	t.Parallel()
	c := NewConfig()
	c.Meta.RetentionAutoCreate = false
	s := OpenServer(c)
	defer s.Close()

	test := tests.load(t, "retention_policy_commands")

	// Create a database.
	if _, err := s.CreateDatabase(test.database()); err != nil {
		t.Fatal(err)
	}

	for _, query := range test.queries {
		t.Run(query.name, func(t *testing.T) {
			if query.skip {
				t.Skipf("SKIP:: %s", query.name)
			}
			if err := query.Execute(s); err != nil {
				t.Error(query.Error(err))
			} else if !query.success() {
				t.Error(query.failureMessage())
			}
		})
	}
}

func TestServer_DatabaseRetentionPolicyAutoCreate(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig())
	defer s.Close()

	test := tests.load(t, "retention_policy_auto_create")

	for _, query := range test.queries {
		t.Run(query.name, func(t *testing.T) {
			if query.skip {
				t.Skipf("SKIP:: %s", query.name)
			}
			if err := query.Execute(s); err != nil {
				t.Error(query.Error(err))
			} else if !query.success() {
				t.Error(query.failureMessage())
			}
		})
	}
}

func TestServer_ShowDatabases_NoAuth(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig())
	defer s.Close()

	test := tests.load(t, "show_database_no_auth")

	for _, query := range test.queries {
		t.Run(query.name, func(t *testing.T) {
			if query.skip {
				t.Skipf("SKIP:: %s", query.name)
			}
			if err := query.Execute(s); err != nil {
				t.Error(fmt.Sprintf("command: %s - err: %s", query.command, query.Error(err)))
			} else if !query.success() {
				t.Error(query.failureMessage())
			}
		})
	}
}

// Ensure the server can create a single point via line protocol with float type and read it back.
func TestServer_Write_LineProtocol_Float(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig())
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("rp0", 1, 1*time.Hour), true); err != nil {
		t.Fatal(err)
	}

	now := now()
	if res, err := s.Write("db0", "rp0", `cpu,host=server01 value=1.0 `+strconv.FormatInt(now.UnixNano(), 10), nil); err != nil {
		t.Fatal(err)
	} else if exp := ``; exp != res {
		t.Fatalf("unexpected results\nexp: %s\ngot: %s\n", exp, res)
	}

	// Verify the data was written.
	if res, err := s.Query(`SELECT * FROM db0.rp0.cpu GROUP BY *`); err != nil {
		t.Fatal(err)
	} else if exp := fmt.Sprintf(`{"results":[{"statement_id":0,"series":[{"name":"cpu","tags":{"host":"server01"},"columns":["time","value"],"values":[["%s",1]]}]}]}`, now.Format(time.RFC3339Nano)); exp != res {
		t.Fatalf("unexpected results\nexp: %s\ngot: %s\n", exp, res)
	}
}
