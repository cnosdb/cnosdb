package tests

import (
	"flag"
	"fmt"
	"math/rand"
	"os"
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

// Ensure user commands work.
func TestServer_UserCommands(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig())
	defer s.Close()

	// Create a database.
	if _, err := s.CreateDatabase("db0"); err != nil {
		t.Fatal(err)
	}

	test := Test{
		queries: []*Query{
			&Query{
				name:    "show users, no actual users",
				command: `SHOW USERS`,
				exp:     `{"results":[{"statement_id":0,"series":[{"columns":["user","admin"]}]}]}`,
			},
			&Query{
				name:    `create user`,
				command: "CREATE USER jdoe WITH PASSWORD '1337'",
				exp:     `{"results":[{"statement_id":0}]}`,
			},
			&Query{
				name:    "show users, 1 existing user",
				command: `SHOW USERS`,
				exp:     `{"results":[{"statement_id":0,"series":[{"columns":["user","admin"],"values":[["jdoe",false]]}]}]}`,
			},
			&Query{
				name:    "grant all priviledges to jdoe",
				command: `GRANT ALL PRIVILEGES TO jdoe`,
				exp:     `{"results":[{"statement_id":0}]}`,
			},
			&Query{
				name:    "show users, existing user as admin",
				command: `SHOW USERS`,
				exp:     `{"results":[{"statement_id":0,"series":[{"columns":["user","admin"],"values":[["jdoe",true]]}]}]}`,
			},
			&Query{
				name:    "grant DB privileges to user",
				command: `GRANT READ ON db0 TO jdoe`,
				exp:     `{"results":[{"statement_id":0}]}`,
			},
			&Query{
				name:    "revoke all privileges",
				command: `REVOKE ALL PRIVILEGES FROM jdoe`,
				exp:     `{"results":[{"statement_id":0}]}`,
			},
			&Query{
				name:    "bad create user request",
				command: `CREATE USER 0xBAD WITH PASSWORD pwd1337`,
				exp:     `{"error":"error parsing query: found 0xBAD, expected identifier at line 1, char 13"}`,
			},
			&Query{
				name:    "bad create user request, no name",
				command: `CREATE USER WITH PASSWORD pwd1337`,
				exp:     `{"error":"error parsing query: found WITH, expected identifier at line 1, char 13"}`,
			},
			&Query{
				name:    "bad create user request, no password",
				command: `CREATE USER jdoe`,
				exp:     `{"error":"error parsing query: found EOF, expected WITH at line 1, char 18"}`,
			},
			&Query{
				name:    "drop user",
				command: `DROP USER jdoe`,
				exp:     `{"results":[{"statement_id":0}]}`,
			},
			&Query{
				name:    "make sure user was dropped",
				command: `SHOW USERS`,
				exp:     `{"results":[{"statement_id":0,"series":[{"columns":["user","admin"]}]}]}`,
			},
			&Query{
				name:    "delete non existing user",
				command: `DROP USER noone`,
				exp:     `{"results":[{"statement_id":0,"error":"user not found"}]}`,
			},
		},
	}

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
