package tests

import (
	"flag"
	"fmt"
	"math/rand"
	"net/url"
	"os"
	"strconv"
	"strings"
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
	if RemoteEnabled() {
		t.Skip("Skipping.  Cannot run on remote server")
	}

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

	if _, ok := s.(*RemoteServer); ok {
		t.Skip("Skipping. Cannot alter auto create rp remotely")
	}

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

	test := tests.load(t, "user_commands")

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
	if res, err := s.Write("db0", "rp0", `air,station=XiaoMaiDao visibility=68.0 `+strconv.FormatInt(now.UnixNano(), 10), nil); err != nil {
		t.Fatal(err)
	} else if exp := ``; exp != res {
		t.Fatalf("unexpected results\nexp: %s\ngot: %s\n", exp, res)
	}

	// Verify the data was written.
	if res, err := s.Query(`SELECT * FROM db0.rp0.air GROUP BY *`); err != nil {
		t.Fatal(err)
	} else if exp := fmt.Sprintf(`{"results":[{"statement_id":0,"series":[{"name":"air","tags":{"station":"XiaoMaiDao"},"columns":["time","visibility"],"values":[["%s",68]]}]}]}`, now.Format(time.RFC3339Nano)); exp != res {
		t.Fatalf("unexpected results\nexp: %s\ngot: %s\n", exp, res)
	}
}

func TestServer_Write_LineProtocol_Bool(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig())
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("rp0", 1, 1*time.Hour), true); err != nil {
		t.Fatal(err)
	}

	now := now()
	if res, err := s.Write("db0", "rp0", `air,station=XiaoMaiDao value=true `+strconv.FormatInt(now.UnixNano(), 10), nil); err != nil {
		t.Fatal(err)
	} else if exp := ``; exp != res {
		t.Fatalf("unexpected results\nexp: %s\ngot: %s\n", exp, res)
	}

	// Verify the data was written.
	if res, err := s.Query(`SELECT * FROM db0.rp0.air GROUP BY *`); err != nil {
		t.Fatal(err)
	} else if exp := fmt.Sprintf(`{"results":[{"statement_id":0,"series":[{"name":"air","tags":{"station":"XiaoMaiDao"},"columns":["time","value"],"values":[["%s",true]]}]}]}`, now.Format(time.RFC3339Nano)); exp != res {
		t.Fatalf("unexpected results\nexp: %s\ngot: %s\n", exp, res)
	}
}

func TestServer_Write_LineProtocol_String(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig())
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("rp0", 1, 1*time.Hour), true); err != nil {
		t.Fatal(err)
	}

	now := now()
	if res, err := s.Write("db0", "rp0", `air,station=XiaoMaiDao temperature="scorching hot" `+strconv.FormatInt(now.UnixNano(), 10), nil); err != nil {
		t.Fatal(err)
	} else if exp := ``; exp != res {
		t.Fatalf("unexpected results\nexp: %s\ngot: %s\n", exp, res)
	}

	// Verify the data was written.
	if res, err := s.Query(`SELECT * FROM db0.rp0.air GROUP BY *`); err != nil {
		t.Fatal(err)
	} else if exp := fmt.Sprintf(`{"results":[{"statement_id":0,"series":[{"name":"air","tags":{"station":"XiaoMaiDao"},"columns":["time","temperature"],"values":[["%s","scorching hot"]]}]}]}`, now.Format(time.RFC3339Nano)); exp != res {
		t.Fatalf("unexpected results\nexp: %s\ngot: %s\n", exp, res)
	}
}

func TestServer_Write_LineProtocol_Integer(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig())
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("rp0", 1, 1*time.Hour), true); err != nil {
		t.Fatal(err)
	}

	now := now()
	if res, err := s.Write("db0", "rp0", `air,station=XiaoMaiDao pressure=75i `+strconv.FormatInt(now.UnixNano(), 10), nil); err != nil {
		t.Fatal(err)
	} else if exp := ``; exp != res {
		t.Fatalf("unexpected results\nexp: %s\ngot: %s\n", exp, res)
	}

	// Verify the data was written.
	if res, err := s.Query(`SELECT * FROM db0.rp0.air GROUP BY *`); err != nil {
		t.Fatal(err)
	} else if exp := fmt.Sprintf(`{"results":[{"statement_id":0,"series":[{"name":"air","tags":{"station":"XiaoMaiDao"},"columns":["time","pressure"],"values":[["%s",75]]}]}]}`, now.Format(time.RFC3339Nano)); exp != res {
		t.Fatalf("unexpected results\nexp: %s\ngot: %s\n", exp, res)
	}
}

func TestServer_Write_LineProtocol_Unsigned(t *testing.T) {
	if RemoteEnabled() {
		t.Skip("Skipping.  Cannot run on remote server")
	}

	t.Parallel()
	s := OpenServer(NewConfig())
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("rp0", 1, 1*time.Hour), true); err != nil {
		t.Fatal(err)
	}

	now := now()
	//100u not support
	if res, err := s.Write("db0", "rp0", `air,station=XiaoMaiDao pressure=75 `+strconv.FormatInt(now.UnixNano(), 10), nil); err != nil {
		t.Fatal(err)
	} else if exp := ``; exp != res {
		t.Fatalf("unexpected results\nexp: %s\ngot: %s\n", exp, res)
	}

	// Verify the data was written.
	if res, err := s.Query(`SELECT * FROM db0.rp0.air GROUP BY *`); err != nil {
		t.Fatal(err)
	} else if exp := fmt.Sprintf(`{"results":[{"statement_id":0,"series":[{"name":"air","tags":{"station":"XiaoMaiDao"},"columns":["time","pressure"],"values":[["%s",75]]}]}]}`, now.Format(time.RFC3339Nano)); exp != res {
		t.Fatalf("unexpected results\nexp: %s\ngot: %s\n", exp, res)
	}
}

func TestServer_Write_LineProtocol_Partial(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig())
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("rp0", 1, 1*time.Hour), true); err != nil {
		t.Fatal(err)
	}

	now := now()
	points := []string{
		"air,station=XiaoMaiDao pressure=75 " + strconv.FormatInt(now.UnixNano(), 10),
		"air,station=XiaoMaiDao pressure=NaN " + strconv.FormatInt(now.UnixNano(), 20),
		"air,station=XiaoMaiDao pressure=NaN " + strconv.FormatInt(now.UnixNano(), 30),
	}
	if res, err := s.Write("db0", "rp0", strings.Join(points, "\n"), nil); err == nil {
		t.Fatal("expected error. got nil", err)
	} else if exp := ``; exp != res {
		t.Fatalf("unexpected results\nexp: %s\ngot: %s\n", exp, res)
	} else if exp := "partial write"; !strings.Contains(err.Error(), exp) {
		t.Fatalf("unexpected error: exp\nexp: %v\ngot: %v", exp, err)
	}

	// Verify the data was written.
	if res, err := s.Query(`SELECT * FROM db0.rp0.air GROUP BY *`); err != nil {
		t.Fatal(err)
	} else if exp := fmt.Sprintf(`{"results":[{"statement_id":0,"series":[{"name":"air","tags":{"station":"XiaoMaiDao"},"columns":["time","pressure"],"values":[["%s",75]]}]}]}`, now.Format(time.RFC3339Nano)); exp != res {
		t.Fatalf("unexpected results\nexp: %s\ngot: %s\n", exp, res)
	}
}

// Ensure the server can query with default databases (via param) and default retention policy
func TestServer_Query_DefaultDBAndRP(t *testing.T) {

	t.Parallel()
	s := OpenServer(NewConfig())
	defer s.Close()

	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: fmt.Sprintf(`air value=1.0 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T01:00:00Z").UnixNano())},
	}

	test.addQueries([]*Query{
		&Query{
			name:    "default db and rp",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT * FROM air GROUP BY *`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","value"],"values":[["2000-01-01T01:00:00Z",1]]}]}]}`,
		},
		&Query{
			name:    "default rp exists",
			command: `show retention policies ON db0`,
			exp:     `{"results":[{"statement_id":0,"series":[{"columns":["name","duration","groupDuration","replicaN","default"],"values":[["autogen","0s","168h0m0s",1,false],["rp0","0s","168h0m0s",1,true]]}]}]}`,
		},
		&Query{
			name:    "default rp",
			command: `SELECT * FROM db0..air GROUP BY *`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","value"],"values":[["2000-01-01T01:00:00Z",1]]}]}]}`,
		},
		&Query{
			name:    "default dp",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT * FROM rp0.air GROUP BY *`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","value"],"values":[["2000-01-01T01:00:00Z",1]]}]}]}`,
		},
	}...)

	if err := test.init(s); err != nil {
		t.Fatalf("test init failed: %s", err)
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

func TestServer_ShowShardsNonInf(t *testing.T) {

	if RemoteEnabled() {
		t.Skip("Skipping.  Cannot run on remote server")
	}

	t.Parallel()
	s := OpenServer(NewConfig())
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("rp0", 1, 1000000*time.Hour), true); err != nil {
		t.Fatal(err)
	}

	points := []string{
		"air,station=XiaoMaiDao pressure=75 1621440001000000000",
	}
	if _, err := s.Write("db0", "rp0", strings.Join(points, "\n"), nil); err != nil {
		t.Fatal("unexpected error: ", err)
	}

	if err := s.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("rp1", 1, 0), true); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Write("db0", "rp1", strings.Join(points, "\n"), nil); err != nil {
		t.Fatal("unexpected error: ", err)
	}

	// inf shard has no expiry_time, shard with expiry has correct expiry_time
	exp := `{"results":[{"statement_id":0,"series":[{"name":"db0","columns":` +
		`["id","database","rp","shard_group","start_time","end_time","expiry_time","owners"],"values":[` +
		`[2,"db0","rp0",1,"2021-05-17T00:00:00Z","2021-05-24T00:00:00Z","2135-06-22T16:00:00Z","0"],` +
		`[4,"db0","rp1",2,"2021-05-17T00:00:00Z","2021-05-24T00:00:00Z","2021-05-24T00:00:00Z","0"]]}]}]}`
	// Verify the data was written.
	if res, err := s.Query(`show shards`); err != nil {
		t.Fatal(err)
	} else if exp != res {
		t.Fatalf("unexpected results\nexp: %s\ngot: %s\n", exp, res)
	}
}

func TestServer_Query_Multiple_Measurements(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig())
	defer s.Close()

	// Make sure we do writes for measurements that will span across shards
	writes := []string{
		fmt.Sprintf("air,station=XiaoMaiDao temperature=100,pressure=40 %d", mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00Z").UnixNano()),
		fmt.Sprintf("air1,station=LianYunGang temperature=50,pressure=20 %d", mustParseTime(time.RFC3339Nano, "2015-01-01T00:00:00Z").UnixNano()),
	}
	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}

	test.addQueries([]*Query{
		&Query{
			name:    "measurement in one shard but not another shouldn't panic server",
			command: `SELECT station,temperature  FROM db0.rp0.air`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","station","temperature"],"values":[["2000-01-01T00:00:00Z","XiaoMaiDao",100]]}]}]}`,
		},
		&Query{
			name:    "measurement in one shard but not another shouldn't panic server",
			command: `SELECT station,temperature  FROM db0.rp0.air GROUP BY station`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","tags":{"station":"XiaoMaiDao"},"columns":["time","station","temperature"],"values":[["2000-01-01T00:00:00Z","XiaoMaiDao",100]]}]}]}`,
		},
	}...)

	if err := test.init(s); err != nil {
		t.Fatalf("test init failed: %s", err)
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

func TestServer_Query_IdenticalTagValues(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig())
	defer s.Close()

	writes := []string{
		fmt.Sprintf("air,t1=val1 temperature=1 %d", mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00Z").UnixNano()),
		fmt.Sprintf("air,t2=val2 temperature=2 %d", mustParseTime(time.RFC3339Nano, "2000-01-01T00:01:00Z").UnixNano()),
		fmt.Sprintf("air,t1=val2 temperature=3 %d", mustParseTime(time.RFC3339Nano, "2000-01-01T00:02:00Z").UnixNano()),
	}
	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}

	test.addQueries([]*Query{
		&Query{
			name:    "measurements with identical tag values - SELECT *, no GROUP BY",
			command: `SELECT * FROM db0.rp0.air GROUP BY *`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","tags":{"t1":"","t2":"val2"},"columns":["time","temperature"],"values":[["2000-01-01T00:01:00Z",2]]},{"name":"air","tags":{"t1":"val1","t2":""},"columns":["time","temperature"],"values":[["2000-01-01T00:00:00Z",1]]},{"name":"air","tags":{"t1":"val2","t2":""},"columns":["time","temperature"],"values":[["2000-01-01T00:02:00Z",3]]}]}]}`,
		},
		&Query{
			name:    "measurements with identical tag values - SELECT *, with GROUP BY",
			command: `SELECT temperature FROM db0.rp0.air GROUP BY t1,t2`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","tags":{"t1":"","t2":"val2"},"columns":["time","temperature"],"values":[["2000-01-01T00:01:00Z",2]]},{"name":"air","tags":{"t1":"val1","t2":""},"columns":["time","temperature"],"values":[["2000-01-01T00:00:00Z",1]]},{"name":"air","tags":{"t1":"val2","t2":""},"columns":["time","temperature"],"values":[["2000-01-01T00:02:00Z",3]]}]}]}`,
		},
		&Query{
			name:    "measurements with identical tag values - SELECT value no GROUP BY",
			command: `SELECT temperature FROM db0.rp0.air`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","temperature"],"values":[["2000-01-01T00:00:00Z",1],["2000-01-01T00:01:00Z",2],["2000-01-01T00:02:00Z",3]]}]}]}`,
		},
	}...)

	if err := test.init(s); err != nil {
		t.Fatalf("test init failed: %s", err)
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

func TestServer_Query_NonExistent(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig())
	defer s.Close()

	now := now()

	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: `air,station=XiaoMaiDao temperature=100 ` + strconv.FormatInt(now.UnixNano(), 10)},
	}

	test.addQueries([]*Query{
		&Query{
			name:    "selecting value should succeed",
			command: `SELECT temperature FROM db0.rp0.air`,
			exp:     fmt.Sprintf(`{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","temperature"],"values":[["%s",100]]}]}]}`, now.Format(time.RFC3339Nano)),
		},
		&Query{
			name:    "selecting non-existent should succeed",
			command: `SELECT foo FROM db0.rp0.air`,
			exp:     `{"results":[{"statement_id":0}]}`,
		},
	}...)

	if err := test.init(s); err != nil {
		t.Fatalf("test init failed: %s", err)
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

func TestServer_Query_Math(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig())
	defer s.Close()

	now := now()
	writes := []string{
		"float value=42 " + strconv.FormatInt(now.UnixNano(), 10),
		"integer value=42i " + strconv.FormatInt(now.UnixNano(), 10),
	}

	test := NewTest("db", "rp")
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}

	test.addQueries([]*Query{
		&Query{
			name:    "SELECT multiple of float value",
			command: `SELECT value * 2 from db.rp.float`,
			exp:     fmt.Sprintf(`{"results":[{"statement_id":0,"series":[{"name":"float","columns":["time","value"],"values":[["%s",84]]}]}]}`, now.Format(time.RFC3339Nano)),
		},
		&Query{
			name:    "SELECT multiple of float value",
			command: `SELECT 2 * value from db.rp.float`,
			exp:     fmt.Sprintf(`{"results":[{"statement_id":0,"series":[{"name":"float","columns":["time","value"],"values":[["%s",84]]}]}]}`, now.Format(time.RFC3339Nano)),
		},
		&Query{
			name:    "SELECT multiple of integer value",
			command: `SELECT value * 2 from db.rp.integer`,
			exp:     fmt.Sprintf(`{"results":[{"statement_id":0,"series":[{"name":"integer","columns":["time","value"],"values":[["%s",84]]}]}]}`, now.Format(time.RFC3339Nano)),
		},
		&Query{
			name:    "SELECT float multiple of integer value",
			command: `SELECT value * 2.0 from db.rp.integer`,
			exp:     fmt.Sprintf(`{"results":[{"statement_id":0,"series":[{"name":"integer","columns":["time","value"],"values":[["%s",84]]}]}]}`, now.Format(time.RFC3339Nano)),
		},
		&Query{
			name:    "SELECT square of float value",
			command: `SELECT value * value from db.rp.float`,
			exp:     fmt.Sprintf(`{"results":[{"statement_id":0,"series":[{"name":"float","columns":["time","value_value"],"values":[["%s",1764]]}]}]}`, now.Format(time.RFC3339Nano)),
		},
		&Query{
			name:    "SELECT square of integer value",
			command: `SELECT value * value from db.rp.integer`,
			exp:     fmt.Sprintf(`{"results":[{"statement_id":0,"series":[{"name":"integer","columns":["time","value_value"],"values":[["%s",1764]]}]}]}`, now.Format(time.RFC3339Nano)),
		},
		&Query{
			name:    "SELECT square of integer, float value",
			command: `SELECT value * value,float from db.rp.integer`,
			exp:     fmt.Sprintf(`{"results":[{"statement_id":0,"series":[{"name":"integer","columns":["time","value_value","float"],"values":[["%s",1764,null]]}]}]}`, now.Format(time.RFC3339Nano)),
		},
		&Query{
			name:    "SELECT square of integer value with alias",
			command: `SELECT value * value as square from db.rp.integer`,
			exp:     fmt.Sprintf(`{"results":[{"statement_id":0,"series":[{"name":"integer","columns":["time","square"],"values":[["%s",1764]]}]}]}`, now.Format(time.RFC3339Nano)),
		},
		&Query{
			name:    "SELECT sum of aggregates",
			command: `SELECT max(value) + min(value) from db.rp.integer`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"integer","columns":["time","max_min"],"values":[["1970-01-01T00:00:00Z",84]]}]}]}`,
		},
		&Query{
			name:    "SELECT square of enclosed integer value",
			command: `SELECT ((value) * (value)) from db.rp.integer`,
			exp:     fmt.Sprintf(`{"results":[{"statement_id":0,"series":[{"name":"integer","columns":["time","value_value"],"values":[["%s",1764]]}]}]}`, now.Format(time.RFC3339Nano)),
		},
		&Query{
			name:    "SELECT square of enclosed integer value",
			command: `SELECT (value * value) from db.rp.integer`,
			exp:     fmt.Sprintf(`{"results":[{"statement_id":0,"series":[{"name":"integer","columns":["time","value_value"],"values":[["%s",1764]]}]}]}`, now.Format(time.RFC3339Nano)),
		},
	}...)

	if err := test.init(s); err != nil {
		t.Fatalf("test init failed: %s", err)
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

func TestServer_Query_Count(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig())
	defer s.Close()

	now := now()

	test := NewTest("db0", "rp0")
	writes := []string{
		`air,station=XiaoMaiDao temperature=1.0 ` + strconv.FormatInt(now.UnixNano(), 10),
		`wind speed=1.0,direction=2.0 ` + strconv.FormatInt(now.UnixNano(), 10),
	}

	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}

	hour_ago := now.Add(-time.Hour).UTC()

	test.addQueries([]*Query{
		&Query{
			name:    "selecting count(temperature) should succeed",
			command: `SELECT count(temperature) FROM db0.rp0.air`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","count"],"values":[["1970-01-01T00:00:00Z",1]]}]}]}`,
		},
		&Query{
			name:    "selecting count(temperature) with where time should return result",
			command: fmt.Sprintf(`SELECT count(temperature) FROM db0.rp0.air WHERE time >= '%s'`, hour_ago.Format(time.RFC3339Nano)),
			exp:     fmt.Sprintf(`{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","count"],"values":[["%s",1]]}]}]}`, hour_ago.Format(time.RFC3339Nano)),
		},
		&Query{
			name:    "selecting count(temperature) with filter that excludes all results should return 0",
			command: fmt.Sprintf(`SELECT count(temperature) FROM db0.rp0.air WHERE temperature=100 AND time >= '%s'`, hour_ago.Format(time.RFC3339Nano)),
			exp:     `{"results":[{"statement_id":0}]}`,
		},
		&Query{
			name:    "selecting count(speed) with matching filter against value2 should return correct result",
			command: fmt.Sprintf(`SELECT count(speed) FROM db0.rp0.wind WHERE direction=2 AND time >= '%s'`, hour_ago.Format(time.RFC3339Nano)),
			exp:     fmt.Sprintf(`{"results":[{"statement_id":0,"series":[{"name":"wind","columns":["time","count"],"values":[["%s",1]]}]}]}`, hour_ago.Format(time.RFC3339Nano)),
		},
		&Query{
			name:    "selecting count(speed) with non-matching filter against value2 should return correct result",
			command: fmt.Sprintf(`SELECT count(speed) FROM db0.rp0.wind WHERE direction=3 AND time >= '%s'`, hour_ago.Format(time.RFC3339Nano)),
			exp:     `{"results":[{"statement_id":0}]}`,
		},
		&Query{
			name:    "selecting count(*) should expand the wildcard",
			command: `SELECT count(*) FROM db0.rp0.air`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","count_temperature"],"values":[["1970-01-01T00:00:00Z",1]]}]}]}`,
		},
		&Query{
			name:    "selecting count(2) should error",
			command: `SELECT count(2) FROM db0.rp0.air`,
			exp:     `{"results":[{"statement_id":0,"error":"expected field argument in count()"}]}`,
		},
	}...)

	if err := test.init(s); err != nil {
		t.Fatalf("test init failed: %s", err)
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

func TestServer_Query_MaxSelectSeriesN(t *testing.T) {
	t.Parallel()
	config := NewConfig()
	config.Coordinator.MaxSelectSeriesN = 3
	s := OpenServer(config)
	defer s.Close()

	if _, ok := s.(*RemoteServer); ok {
		t.Skip("Skipping.  Cannot modify MaxSelectSeriesN remotely")
	}

	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: `air,station=XiaoMaiDao01 temperature=1.0 0`},
		&Write{data: `air,station=XiaoMaiDao02 temperature=1.0 0`},
		&Write{data: `air,station=XiaoMaiDao03 temperature=1.0 0`},
		&Write{data: `air,station=XiaoMaiDao04 temperature=1.0 0`},
	}

	test.addQueries([]*Query{
		&Query{
			name:    "exceeed max series",
			command: `SELECT COUNT(temperature) FROM db0.rp0.air`,
			exp:     `{"results":[{"statement_id":0,"error":"max-select-series limit exceeded: (4/3)"}]}`,
		},
	}...)

	if err := test.init(s); err != nil {
		t.Fatalf("test init failed: %s", err)
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

func TestServer_Query_Now(t *testing.T) {
	if RemoteEnabled() {
		t.Skip("Skipping.  Cannot run on remote server")
	}

	t.Parallel()
	s := OpenServer(NewConfig())
	defer s.Close()

	now := now()

	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: `air,station=XiaoMaiDao temperature=1.0 ` + strconv.FormatInt(now.UnixNano(), 10)},
	}

	test.addQueries([]*Query{
		&Query{
			name:    "where with time < now() should work",
			command: `SELECT * FROM db0.rp0.air where time < now()`,
			exp:     fmt.Sprintf(`{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","station","temperature"],"values":[["%s","XiaoMaiDao",1]]}]}]}`, now.Format(time.RFC3339Nano)),
		},
		&Query{
			name:    "where with time < now() and GROUP BY * should work",
			command: `SELECT * FROM db0.rp0.air where time < now() GROUP BY *`,
			exp:     fmt.Sprintf(`{"results":[{"statement_id":0,"series":[{"name":"air","tags":{"station":"XiaoMaiDao"},"columns":["time","temperature"],"values":[["%s",1]]}]}]}`, now.Format(time.RFC3339Nano)),
		},
		&Query{
			name:    "where with time > now() should return an empty result",
			command: `SELECT * FROM db0.rp0.air where time > now()`,
			exp:     `{"results":[{"statement_id":0}]}`,
		},
		&Query{
			name:    "where with time > now() with GROUP BY * should return an empty result",
			command: `SELECT * FROM db0.rp0.air where time > now() GROUP BY *`,
			exp:     `{"results":[{"statement_id":0}]}`,
		},
	}...)

	if err := test.init(s); err != nil {
		t.Fatalf("test init failed: %s", err)
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

func TestServer_Query_EpochPrecision(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig())
	defer s.Close()

	now := now()

	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: `air,station=XiaoMaiDao temperature=1.0 ` + strconv.FormatInt(now.UnixNano(), 10)},
	}

	test.addQueries([]*Query{
		&Query{
			name:    "nanosecond precision",
			command: `SELECT * FROM db0.rp0.air GROUP BY *`,
			params:  url.Values{"epoch": []string{"n"}},
			exp:     fmt.Sprintf(`{"results":[{"statement_id":0,"series":[{"name":"air","tags":{"station":"XiaoMaiDao"},"columns":["time","temperature"],"values":[[%d,1]]}]}]}`, now.UnixNano()),
		},
		&Query{
			name:    "microsecond precision",
			command: `SELECT * FROM db0.rp0.air GROUP BY *`,
			params:  url.Values{"epoch": []string{"u"}},
			exp:     fmt.Sprintf(`{"results":[{"statement_id":0,"series":[{"name":"air","tags":{"station":"XiaoMaiDao"},"columns":["time","temperature"],"values":[[%d,1]]}]}]}`, now.UnixNano()/int64(time.Microsecond)),
		},
		&Query{
			name:    "millisecond precision",
			command: `SELECT * FROM db0.rp0.air GROUP BY *`,
			params:  url.Values{"epoch": []string{"ms"}},
			exp:     fmt.Sprintf(`{"results":[{"statement_id":0,"series":[{"name":"air","tags":{"station":"XiaoMaiDao"},"columns":["time","temperature"],"values":[[%d,1]]}]}]}`, now.UnixNano()/int64(time.Millisecond)),
		},
		&Query{
			name:    "second precision",
			command: `SELECT * FROM db0.rp0.air GROUP BY *`,
			params:  url.Values{"epoch": []string{"s"}},
			exp:     fmt.Sprintf(`{"results":[{"statement_id":0,"series":[{"name":"air","tags":{"station":"XiaoMaiDao"},"columns":["time","temperature"],"values":[[%d,1]]}]}]}`, now.UnixNano()/int64(time.Second)),
		},
		&Query{
			name:    "minute precision",
			command: `SELECT * FROM db0.rp0.air GROUP BY *`,
			params:  url.Values{"epoch": []string{"m"}},
			exp:     fmt.Sprintf(`{"results":[{"statement_id":0,"series":[{"name":"air","tags":{"station":"XiaoMaiDao"},"columns":["time","temperature"],"values":[[%d,1]]}]}]}`, now.UnixNano()/int64(time.Minute)),
		},
		&Query{
			name:    "hour precision",
			command: `SELECT * FROM db0.rp0.air GROUP BY *`,
			params:  url.Values{"epoch": []string{"h"}},
			exp:     fmt.Sprintf(`{"results":[{"statement_id":0,"series":[{"name":"air","tags":{"station":"XiaoMaiDao"},"columns":["time","temperature"],"values":[[%d,1]]}]}]}`, now.UnixNano()/int64(time.Hour)),
		},
	}...)

	if err := test.init(s); err != nil {
		t.Fatalf("test init failed: %s", err)
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

func TestServer_Query_Tags(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig())
	defer s.Close()

	now := now()

	writes := []string{
		fmt.Sprintf("air,station=XiaoMaiDao01 temperature=100,pressure=4 %d", now.UnixNano()),
		fmt.Sprintf("air,station=XiaoMaiDao02 temperature=50,pressure=2 %d", now.Add(1).UnixNano()),
		fmt.Sprintf("air1,station=XiaoMaiDao01,region=us-west temperature=100 %d", mustParseTime(time.RFC3339Nano, "2015-02-28T01:03:36.703820946Z").UnixNano()),
		fmt.Sprintf("air1,station=XiaoMaiDao02 temperature=200 %d", mustParseTime(time.RFC3339Nano, "2010-02-28T01:03:37.703820946Z").UnixNano()),
		fmt.Sprintf("air1,station=XiaoMaiDao03 temperature=300 %d", mustParseTime(time.RFC3339Nano, "2012-02-28T01:03:38.703820946Z").UnixNano()),
		fmt.Sprintf("air2,station=XiaoMaiDao01 temperature=100 %d", mustParseTime(time.RFC3339Nano, "2015-02-28T01:03:36.703820946Z").UnixNano()),
		fmt.Sprintf("air2 temperature=200 %d", mustParseTime(time.RFC3339Nano, "2012-02-28T01:03:38.703820946Z").UnixNano()),
		fmt.Sprintf("air3,company=acme01 temperature=100 %d", mustParseTime(time.RFC3339Nano, "2015-02-28T01:03:36.703820946Z").UnixNano()),
		fmt.Sprintf("air3 temperature=200 %d", mustParseTime(time.RFC3339Nano, "2012-02-28T01:03:38.703820946Z").UnixNano()),

		fmt.Sprintf("status_code,url=http://www.example.com value=404 %d", mustParseTime(time.RFC3339Nano, "2015-07-22T08:13:54.929026672Z").UnixNano()),
		fmt.Sprintf("status_code,url=https://cnosdb.com value=418 %d", mustParseTime(time.RFC3339Nano, "2015-07-22T09:52:24.914395083Z").UnixNano()),
	}

	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}

	test.addQueries([]*Query{

		&Query{
			name:    "field with tag should succeed",
			command: `SELECT station, temperature FROM db0.rp0.air`,
			exp:     fmt.Sprintf(`{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","station","temperature"],"values":[["%s","XiaoMaiDao01",100],["%s","XiaoMaiDao02",50]]}]}]}`, now.Format(time.RFC3339Nano), now.Add(1).Format(time.RFC3339Nano)),
		},
		&Query{
			name:    "field with tag and GROUP BY should succeed",
			command: `SELECT station, temperature FROM db0.rp0.air GROUP BY station`,
			exp:     fmt.Sprintf(`{"results":[{"statement_id":0,"series":[{"name":"air","tags":{"station":"XiaoMaiDao01"},"columns":["time","station","temperature"],"values":[["%s","XiaoMaiDao01",100]]},{"name":"air","tags":{"station":"XiaoMaiDao02"},"columns":["time","station","temperature"],"values":[["%s","XiaoMaiDao02",50]]}]}]}`, now.Format(time.RFC3339Nano), now.Add(1).Format(time.RFC3339Nano)),
		},
		&Query{
			name:    "field with two tags should succeed",
			command: `SELECT station, temperature, pressure FROM db0.rp0.air`,
			exp:     fmt.Sprintf(`{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","station","temperature","pressure"],"values":[["%s","XiaoMaiDao01",100,4],["%s","XiaoMaiDao02",50,2]]}]}]}`, now.Format(time.RFC3339Nano), now.Add(1).Format(time.RFC3339Nano)),
		},
		&Query{
			name:    "field with two tags and GROUP BY should succeed",
			command: `SELECT station, temperature, pressure FROM db0.rp0.air GROUP BY station`,
			exp:     fmt.Sprintf(`{"results":[{"statement_id":0,"series":[{"name":"air","tags":{"station":"XiaoMaiDao01"},"columns":["time","station","temperature","pressure"],"values":[["%s","XiaoMaiDao01",100,4]]},{"name":"air","tags":{"station":"XiaoMaiDao02"},"columns":["time","station","temperature","pressure"],"values":[["%s","XiaoMaiDao02",50,2]]}]}]}`, now.Format(time.RFC3339Nano), now.Add(1).Format(time.RFC3339Nano)),
		},
		&Query{
			name:    "select * with tags should succeed",
			command: `SELECT * FROM db0.rp0.air`,
			exp:     fmt.Sprintf(`{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","pressure","station","temperature"],"values":[["%s",4,"XiaoMaiDao01",100],["%s",2,"XiaoMaiDao02",50]]}]}]}`, now.Format(time.RFC3339Nano), now.Add(1).Format(time.RFC3339Nano)),
		},
		&Query{
			name:    "select * with tags with GROUP BY * should succeed",
			command: `SELECT * FROM db0.rp0.air GROUP BY *`,
			exp:     fmt.Sprintf(`{"results":[{"statement_id":0,"series":[{"name":"air","tags":{"station":"XiaoMaiDao01"},"columns":["time","pressure","temperature"],"values":[["%s",4,100]]},{"name":"air","tags":{"station":"XiaoMaiDao02"},"columns":["time","pressure","temperature"],"values":[["%s",2,50]]}]}]}`, now.Format(time.RFC3339Nano), now.Add(1).Format(time.RFC3339Nano)),
		},
		&Query{
			name:    "group by tag",
			command: `SELECT temperature FROM db0.rp0.air GROUP by station`,
			exp:     fmt.Sprintf(`{"results":[{"statement_id":0,"series":[{"name":"air","tags":{"station":"XiaoMaiDao01"},"columns":["time","temperature"],"values":[["%s",100]]},{"name":"air","tags":{"station":"XiaoMaiDao02"},"columns":["time","temperature"],"values":[["%s",50]]}]}]}`, now.Format(time.RFC3339Nano), now.Add(1).Format(time.RFC3339Nano)),
		},
		&Query{
			name:    "single field (EQ tag value1)",
			command: `SELECT temperature FROM db0.rp0.air1 WHERE station = 'XiaoMaiDao01'`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air1","columns":["time","temperature"],"values":[["2015-02-28T01:03:36.703820946Z",100]]}]}]}`,
		},
		&Query{
			name:    "single field (2 EQ tags)",
			command: `SELECT temperature FROM db0.rp0.air1 WHERE station = 'XiaoMaiDao01' AND region = 'us-west'`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air1","columns":["time","temperature"],"values":[["2015-02-28T01:03:36.703820946Z",100]]}]}]}`,
		},
		&Query{
			name:    "single field (OR different tags)",
			command: `SELECT temperature FROM db0.rp0.air1 WHERE station = 'XiaoMaiDao03' OR region = 'us-west'`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air1","columns":["time","temperature"],"values":[["2012-02-28T01:03:38.703820946Z",300],["2015-02-28T01:03:36.703820946Z",100]]}]}]}`,
		},
		&Query{
			name:    "single field (OR with non-existent tag value)",
			command: `SELECT temperature FROM db0.rp0.air1 WHERE station = 'XiaoMaiDao01' OR station = 'XiaoMaiDao66'`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air1","columns":["time","temperature"],"values":[["2015-02-28T01:03:36.703820946Z",100]]}]}]}`,
		},
		&Query{
			name:    "single field (OR with all tag values)",
			command: `SELECT temperature FROM db0.rp0.air1 WHERE station = 'XiaoMaiDao01' OR station = 'XiaoMaiDao02' OR station = 'XiaoMaiDao03'`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air1","columns":["time","temperature"],"values":[["2010-02-28T01:03:37.703820946Z",200],["2012-02-28T01:03:38.703820946Z",300],["2015-02-28T01:03:36.703820946Z",100]]}]}]}`,
		},
		&Query{
			name:    "single field (1 EQ and 1 NEQ tag)",
			command: `SELECT temperature FROM db0.rp0.air1 WHERE station = 'XiaoMaiDao01' AND region != 'us-west'`,
			exp:     `{"results":[{"statement_id":0}]}`,
		},
		&Query{
			name:    "single field (EQ tag value2)",
			command: `SELECT temperature FROM db0.rp0.air1 WHERE station = 'XiaoMaiDao02'`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air1","columns":["time","temperature"],"values":[["2010-02-28T01:03:37.703820946Z",200]]}]}]}`,
		},
		&Query{
			name:    "single field (NEQ tag value1)",
			command: `SELECT temperature FROM db0.rp0.air1 WHERE station != 'XiaoMaiDao01'`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air1","columns":["time","temperature"],"values":[["2010-02-28T01:03:37.703820946Z",200],["2012-02-28T01:03:38.703820946Z",300]]}]}]}`,
		},
		&Query{
			name:    "single field (NEQ tag value1 AND NEQ tag value2)",
			command: `SELECT temperature FROM db0.rp0.air1 WHERE station != 'XiaoMaiDao01' AND station != 'XiaoMaiDao02'`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air1","columns":["time","temperature"],"values":[["2012-02-28T01:03:38.703820946Z",300]]}]}]}`,
		},
		&Query{
			name:    "single field (NEQ tag value1 OR NEQ tag value2)",
			command: `SELECT temperature FROM db0.rp0.air1 WHERE station != 'XiaoMaiDao01' OR station != 'XiaoMaiDao02'`, // Yes, this is always true, but that's the point.
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air1","columns":["time","temperature"],"values":[["2010-02-28T01:03:37.703820946Z",200],["2012-02-28T01:03:38.703820946Z",300],["2015-02-28T01:03:36.703820946Z",100]]}]}]}`,
		},
		&Query{
			name:    "single field (NEQ tag value1 AND NEQ tag value2 AND NEQ tag value3)",
			command: `SELECT temperature FROM db0.rp0.air1 WHERE station != 'XiaoMaiDao01' AND station != 'XiaoMaiDao02' AND station != 'XiaoMaiDao03'`,
			exp:     `{"results":[{"statement_id":0}]}`,
		},
		&Query{
			name:    "single field (NEQ tag value1, point without any tags)",
			command: `SELECT temperature FROM db0.rp0.air2 WHERE station != 'XiaoMaiDao01'`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air2","columns":["time","temperature"],"values":[["2012-02-28T01:03:38.703820946Z",200]]}]}]}`,
		},
		&Query{
			name:    "single field (NEQ tag value1, point without any tags)",
			command: `SELECT temperature FROM db0.rp0.air3 WHERE company !~ /acme01/`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air3","columns":["time","temperature"],"values":[["2012-02-28T01:03:38.703820946Z",200]]}]}]}`,
		},
		&Query{
			name:    "single field (regex tag match)",
			command: `SELECT temperature FROM db0.rp0.air3 WHERE company =~ /acme01/`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air3","columns":["time","temperature"],"values":[["2015-02-28T01:03:36.703820946Z",100]]}]}]}`,
		},
		&Query{
			name:    "single field (regex tag match)",
			command: `SELECT temperature FROM db0.rp0.air3 WHERE company !~ /acme[23]/`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air3","columns":["time","temperature"],"values":[["2012-02-28T01:03:38.703820946Z",200],["2015-02-28T01:03:36.703820946Z",100]]}]}]}`,
		},
		&Query{
			name:    "single field (regex tag match with escaping)",
			command: `SELECT value FROM db0.rp0.status_code WHERE url !~ /https\:\/\/cnosdb\.com/`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"status_code","columns":["time","value"],"values":[["2015-07-22T08:13:54.929026672Z",404]]}]}]}`,
		},
		&Query{
			name:    "single field (regex tag match with escaping)",
			command: `SELECT value FROM db0.rp0.status_code WHERE url =~ /https\:\/\/cnosdb\.com/`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"status_code","columns":["time","value"],"values":[["2015-07-22T09:52:24.914395083Z",418]]}]}]}`,
		},
	}...)

	if err := test.init(s); err != nil {
		t.Fatalf("test init failed: %s", err)
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


func TestServer_Query_Alias(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig())
	defer s.Close()

	writes := []string{
		fmt.Sprintf("air temperature=1i,pressure=3i %d", mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00Z").UnixNano()),
		fmt.Sprintf("air temperature=2i,pressure=4i %d", mustParseTime(time.RFC3339Nano, "2000-01-01T00:01:00Z").UnixNano()),
	}
	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}

	test.addQueries([]*Query{
		&Query{
			name:    "baseline query - SELECT * FROM db0.rp0.air",
			command: `SELECT * FROM db0.rp0.air`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","pressure","temperature"],"values":[["2000-01-01T00:00:00Z",3,1],["2000-01-01T00:01:00Z",4,2]]}]}]}`,
		},
		&Query{
			name:    "basic query with alias - SELECT pressure, temperature as v FROM db0.rp0.air",
			command: `SELECT pressure, temperature as v FROM db0.rp0.air`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","pressure","v"],"values":[["2000-01-01T00:00:00Z",3,1],["2000-01-01T00:01:00Z",4,2]]}]}]}`,
		},
		&Query{
			name:    "double aggregate sum - SELECT sum(temperature), sum(pressure) FROM db0.rp0.air",
			command: `SELECT sum(temperature), sum(pressure) FROM db0.rp0.air`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","sum","sum_1"],"values":[["1970-01-01T00:00:00Z",3,7]]}]}]}`,
		},
		&Query{
			name:    "double aggregate sum reverse order - SELECT sum(pressure), sum(temperature) FROM db0.rp0.air",
			command: `SELECT sum(pressure), sum(temperature) FROM db0.rp0.air`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","sum","sum_1"],"values":[["1970-01-01T00:00:00Z",7,3]]}]}]}`,
		},
		&Query{
			name:    "double aggregate sum with alias - SELECT sum(temperature) as sumv, sum(pressure) as sums FROM db0.rp0.air",
			command: `SELECT sum(temperature) as sumv, sum(pressure) as sums FROM db0.rp0.air`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","sumv","sums"],"values":[["1970-01-01T00:00:00Z",3,7]]}]}]}`,
		},
		&Query{
			name:    "double aggregate with same temperature - SELECT sum(temperature), mean(temperature) FROM db0.rp0.air",
			command: `SELECT sum(temperature), mean(temperature) FROM db0.rp0.air`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","sum","mean"],"values":[["1970-01-01T00:00:00Z",3,1.5]]}]}]}`,
		},
		&Query{
			name:    "double aggregate with same temperature and same alias - SELECT mean(temperature) as mv, max(temperature) as mv FROM db0.rp0.air",
			command: `SELECT mean(temperature) as mv, max(temperature) as mv FROM db0.rp0.air`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","mv","mv"],"values":[["1970-01-01T00:00:00Z",1.5,2]]}]}]}`,
		},
		&Query{
			name:    "double aggregate with non-existent field - SELECT mean(temperature), max(foo) FROM db0.rp0.air",
			command: `SELECT mean(temperature), max(foo) FROM db0.rp0.air`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","mean","max"],"values":[["1970-01-01T00:00:00Z",1.5,null]]}]}]}`,
		},
	}...)

	if err := test.init(s); err != nil {
		t.Fatalf("test init failed: %s", err)
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

// Ensure the server will succeed and error for common scenarios.
func TestServer_Query_Common(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig())
	defer s.Close()

	now := now()

	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: fmt.Sprintf("air,station=XiaoMaiDao01 temperature=1 %s", strconv.FormatInt(now.UnixNano(), 10))},
	}

	test.addQueries([]*Query{
		&Query{
			name:    "selecting a from a non-existent database should error",
			command: `SELECT temperature FROM db1.rp0.air`,
			exp:     `{"results":[{"statement_id":0,"error":"database not found: db1"}]}`,
		},
		&Query{
			name:    "selecting a from a non-existent retention policy should error",
			command: `SELECT temperature FROM db0.rp1.air`,
			exp:     `{"results":[{"statement_id":0,"error":"retention policy not found: rp1"}]}`,
		},
		&Query{
			name:    "selecting a valid  measurement and field should succeed",
			command: `SELECT temperature FROM db0.rp0.air`,
			exp:     fmt.Sprintf(`{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","temperature"],"values":[["%s",1]]}]}]}`, now.Format(time.RFC3339Nano)),
		},
		&Query{
			name:    "explicitly selecting time and a valid measurement and field should succeed",
			command: `SELECT time,temperature FROM db0.rp0.air`,
			exp:     fmt.Sprintf(`{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","temperature"],"values":[["%s",1]]}]}]}`, now.Format(time.RFC3339Nano)),
		},
		&Query{
			name:    "selecting a measurement that doesn't exist should result in empty set",
			command: `SELECT temperature FROM db0.rp0.idontexist`,
			exp:     `{"results":[{"statement_id":0}]}`,
		},
		&Query{
			name:    "selecting a field that doesn't exist should result in empty set",
			command: `SELECT idontexist FROM db0.rp0.air`,
			exp:     `{"results":[{"statement_id":0}]}`,
		},
		&Query{
			name:    "selecting wildcard without specifying a database should error",
			command: `SELECT * FROM air`,
			exp:     `{"results":[{"statement_id":0,"error":"database name required"}]}`,
		},
		&Query{
			name:    "selecting explicit field without specifying a database should error",
			command: `SELECT temperature FROM air`,
			exp:     `{"results":[{"statement_id":0,"error":"database name required"}]}`,
		},
	}...)

	if err := test.init(s); err != nil {
		t.Fatalf("test init failed: %s", err)
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

// Ensure the server can query two points.
func TestServer_Query_SelectTwoPoints(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig())
	defer s.Close()

	now := now()

	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: fmt.Sprintf("air temperature=100 %s\nair temperature=200 %s", strconv.FormatInt(now.UnixNano(), 10), strconv.FormatInt(now.Add(1).UnixNano(), 10))},
	}

	test.addQueries(
		&Query{
			name:    "selecting two points should result in two points",
			command: `SELECT * FROM db0.rp0.air`,
			exp:     fmt.Sprintf(`{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","temperature"],"values":[["%s",100],["%s",200]]}]}]}`, now.Format(time.RFC3339Nano), now.Add(1).Format(time.RFC3339Nano)),
		},
		&Query{
			name:    "selecting two points with GROUP BY * should result in two points",
			command: `SELECT * FROM db0.rp0.air GROUP BY *`,
			exp:     fmt.Sprintf(`{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","temperature"],"values":[["%s",100],["%s",200]]}]}]}`, now.Format(time.RFC3339Nano), now.Add(1).Format(time.RFC3339Nano)),
		},
	)

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

// Ensure the server can query two negative points.
func TestServer_Query_SelectTwoNegativePoints(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig())
	defer s.Close()

	now := now()

	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: fmt.Sprintf("air temperature=-100 %s\nair temperature=-200 %s", strconv.FormatInt(now.UnixNano(), 10), strconv.FormatInt(now.Add(1).UnixNano(), 10))},
	}

	test.addQueries(&Query{
		name:    "selecting two negative points should succeed",
		command: `SELECT * FROM db0.rp0.air`,
		exp:     fmt.Sprintf(`{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","temperature"],"values":[["%s",-100],["%s",-200]]}]}]}`, now.Format(time.RFC3339Nano), now.Add(1).Format(time.RFC3339Nano)),
	})

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


// Ensure the server can query with relative time.
func TestServer_Query_SelectRelativeTime(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig())
	defer s.Close()

	now := now()
	yesterday := yesterday()

	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: fmt.Sprintf("air,station=XiaoMaiDao01 temperature=100 %s\nair,station=XiaoMaiDao01 temperature=200 %s", strconv.FormatInt(yesterday.UnixNano(), 10), strconv.FormatInt(now.UnixNano(), 10))},
	}

	test.addQueries([]*Query{
		&Query{
			name:    "single point with time pre-calculated for past time queries yesterday",
			command: `SELECT * FROM db0.rp0.air where time >= '` + yesterday.Add(-1*time.Minute).Format(time.RFC3339Nano) + `' GROUP BY *`,
			exp:     fmt.Sprintf(`{"results":[{"statement_id":0,"series":[{"name":"air","tags":{"station":"XiaoMaiDao01"},"columns":["time","temperature"],"values":[["%s",100],["%s",200]]}]}]}`, yesterday.Format(time.RFC3339Nano), now.Format(time.RFC3339Nano)),
		},
		&Query{
			name:    "single point with time pre-calculated for relative time queries now",
			command: `SELECT * FROM db0.rp0.air where time >= now() - 1m GROUP BY *`,
			exp:     fmt.Sprintf(`{"results":[{"statement_id":0,"series":[{"name":"air","tags":{"station":"XiaoMaiDao01"},"columns":["time","temperature"],"values":[["%s",200]]}]}]}`, now.Format(time.RFC3339Nano)),
		},
	}...)

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

// Ensure the server can handle various simple derivative queries.
func TestServer_Query_SelectRawDerivative(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig())
	defer s.Close()

	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: fmt.Sprintf("air temperature=210 1278010021000000000\nair temperature=10 1278010022000000000")},
	}

	test.addQueries([]*Query{
		&Query{
			name:    "calculate single derivate",
			command: `SELECT derivative(temperature) from db0.rp0.air`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","derivative"],"values":[["2010-07-01T18:47:02Z",-200]]}]}]}`,
		},
		&Query{
			name:    "calculate derivate with unit",
			command: `SELECT derivative(temperature, 10s) from db0.rp0.air`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","derivative"],"values":[["2010-07-01T18:47:02Z",-2000]]}]}]}`,
		},
	}...)

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

// Ensure the server can handle various simple non_negative_derivative queries.
func TestServer_Query_SelectRawNonNegativeDerivative(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig())
	defer s.Close()

	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: fmt.Sprintf(`air temperature=10 1278010021000000000
air temperature=15 1278010022000000000
air temperature=10 1278010023000000000
air temperature=20 1278010024000000000
`)},
	}

	test.addQueries([]*Query{
		&Query{
			name:    "calculate single non_negative_derivative",
			command: `SELECT non_negative_derivative(temperature) from db0.rp0.air`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","non_negative_derivative"],"values":[["2010-07-01T18:47:02Z",5],["2010-07-01T18:47:04Z",10]]}]}]}`,
		},
		&Query{
			name:    "calculate single non_negative_derivative",
			command: `SELECT non_negative_derivative(temperature, 10s) from db0.rp0.air`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","non_negative_derivative"],"values":[["2010-07-01T18:47:02Z",50],["2010-07-01T18:47:04Z",100]]}]}]}`,
		},
	}...)

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


// Ensure the server can handle various group by time derivative queries.
func TestServer_Query_SelectGroupByTimeDerivative(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig())
	defer s.Close()

	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: fmt.Sprintf(`air temperature=10 1278010020000000000
air temperature=15 1278010021000000000
air temperature=20 1278010022000000000
air temperature=25 1278010023000000000

air0,station=XiaoMaiDao01 visibility=10,pressure=100 1278010020000000000
air0,station=XiaoMaiDao01 visibility=30,pressure=100 1278010021000000000
air0,station=XiaoMaiDao01 visibility=32,pressure=100 1278010022000000000
air0,station=XiaoMaiDao01 visibility=47,pressure=100 1278010023000000000
air0,station=XiaoMaiDao02 visibility=40,pressure=100 1278010020000000000
air0,station=XiaoMaiDao02 visibility=45,pressure=100 1278010021000000000
air0,station=XiaoMaiDao02 visibility=84,pressure=100 1278010022000000000
air0,station=XiaoMaiDao02 visibility=101,pressure=100 1278010023000000000
`)},
	}

	test.addQueries([]*Query{
		&Query{
			name:    "calculate derivative of count with unit default (2s) group by time",
			command: `SELECT derivative(count(temperature)) from db0.rp0.air where time >= '2010-07-01 18:47:00' and time <= '2010-07-01 18:47:03' group by time(2s)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","derivative"],"values":[["2010-07-01T18:47:00Z",2],["2010-07-01T18:47:02Z",0]]}]}]}`,
		},
		&Query{
			name:    "calculate derivative of count with unit 4s group by time",
			command: `SELECT derivative(count(temperature), 4s) from db0.rp0.air where time >= '2010-07-01 18:47:00' and time <= '2010-07-01 18:47:03' group by time(2s)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","derivative"],"values":[["2010-07-01T18:47:00Z",4],["2010-07-01T18:47:02Z",0]]}]}]}`,
		},
		&Query{
			name:    "calculate derivative of mean with unit default (2s) group by time",
			command: `SELECT derivative(mean(temperature)) from db0.rp0.air where time >= '2010-07-01 18:47:00' and time <= '2010-07-01 18:47:03' group by time(2s)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","derivative"],"values":[["2010-07-01T18:47:02Z",10]]}]}]}`,
		},
		&Query{
			name:    "calculate derivative of mean with unit 4s group by time",
			command: `SELECT derivative(mean(temperature), 4s) from db0.rp0.air where time >= '2010-07-01 18:47:00' and time <= '2010-07-01 18:47:03' group by time(2s)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","derivative"],"values":[["2010-07-01T18:47:02Z",20]]}]}]}`,
		},
		&Query{
			name:    "calculate derivative of median with unit default (2s) group by time",
			command: `SELECT derivative(median(temperature)) from db0.rp0.air where time >= '2010-07-01 18:47:00' and time <= '2010-07-01 18:47:03' group by time(2s)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","derivative"],"values":[["2010-07-01T18:47:02Z",10]]}]}]}`,
		},
		&Query{
			name:    "calculate derivative of median with unit 4s group by time",
			command: `SELECT derivative(median(temperature), 4s) from db0.rp0.air where time >= '2010-07-01 18:47:00' and time <= '2010-07-01 18:47:03' group by time(2s)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","derivative"],"values":[["2010-07-01T18:47:02Z",20]]}]}]}`,
		},
		&Query{
			name:    "calculate derivative of mode with unit default (2s) group by time",
			command: `SELECT derivative(mode(temperature)) from db0.rp0.air where time >= '2010-07-01 18:47:00' and time <= '2010-07-01 18:47:03' group by time(2s)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","derivative"],"values":[["2010-07-01T18:47:02Z",10]]}]}]}`,
		},
		&Query{
			name:    "calculate derivative of mode with unit 4s group by time",
			command: `SELECT derivative(mode(temperature), 4s) from db0.rp0.air where time >= '2010-07-01 18:47:00' and time <= '2010-07-01 18:47:03' group by time(2s)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","derivative"],"values":[["2010-07-01T18:47:02Z",20]]}]}]}`,
		},

		&Query{
			name:    "calculate derivative of sum with unit default (2s) group by time",
			command: `SELECT derivative(sum(temperature)) from db0.rp0.air where time >= '2010-07-01 18:47:00' and time <= '2010-07-01 18:47:03' group by time(2s)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","derivative"],"values":[["2010-07-01T18:47:02Z",20]]}]}]}`,
		},
		&Query{
			name:    "calculate derivative of sum with unit 4s group by time",
			command: `SELECT derivative(sum(temperature), 4s) from db0.rp0.air where time >= '2010-07-01 18:47:00' and time <= '2010-07-01 18:47:03' group by time(2s)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","derivative"],"values":[["2010-07-01T18:47:02Z",40]]}]}]}`,
		},
		&Query{
			name:    "calculate derivative of first with unit default (2s) group by time",
			command: `SELECT derivative(first(temperature)) from db0.rp0.air where time >= '2010-07-01 18:47:00' and time <= '2010-07-01 18:47:03' group by time(2s)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","derivative"],"values":[["2010-07-01T18:47:02Z",10]]}]}]}`,
		},
		&Query{
			name:    "calculate derivative of first with unit 4s group by time",
			command: `SELECT derivative(first(temperature), 4s) from db0.rp0.air where time >= '2010-07-01 18:47:00' and time <= '2010-07-01 18:47:03' group by time(2s)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","derivative"],"values":[["2010-07-01T18:47:02Z",20]]}]}]}`,
		},
		&Query{
			name:    "calculate derivative of last with unit default (2s) group by time",
			command: `SELECT derivative(last(temperature)) from db0.rp0.air where time >= '2010-07-01 18:47:00' and time <= '2010-07-01 18:47:03' group by time(2s)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","derivative"],"values":[["2010-07-01T18:47:02Z",10]]}]}]}`,
		},
		&Query{
			name:    "calculate derivative of last with unit 4s group by time",
			command: `SELECT derivative(last(temperature), 4s) from db0.rp0.air where time >= '2010-07-01 18:47:00' and time <= '2010-07-01 18:47:03' group by time(2s)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","derivative"],"values":[["2010-07-01T18:47:02Z",20]]}]}]}`,
		},
		&Query{
			name:    "calculate derivative of min with unit default (2s) group by time",
			command: `SELECT derivative(min(temperature)) from db0.rp0.air where time >= '2010-07-01 18:47:00' and time <= '2010-07-01 18:47:03' group by time(2s)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","derivative"],"values":[["2010-07-01T18:47:02Z",10]]}]}]}`,
		},
		&Query{
			name:    "calculate derivative of min with unit 4s group by time",
			command: `SELECT derivative(min(temperature), 4s) from db0.rp0.air where time >= '2010-07-01 18:47:00' and time <= '2010-07-01 18:47:03' group by time(2s)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","derivative"],"values":[["2010-07-01T18:47:02Z",20]]}]}]}`,
		},
		&Query{
			name:    "calculate derivative of max with unit default (2s) group by time",
			command: `SELECT derivative(max(temperature)) from db0.rp0.air where time >= '2010-07-01 18:47:00' and time <= '2010-07-01 18:47:03' group by time(2s)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","derivative"],"values":[["2010-07-01T18:47:02Z",10]]}]}]}`,
		},
		&Query{
			name:    "calculate derivative of max with unit 4s group by time",
			command: `SELECT derivative(max(temperature), 4s) from db0.rp0.air where time >= '2010-07-01 18:47:00' and time <= '2010-07-01 18:47:03' group by time(2s)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","derivative"],"values":[["2010-07-01T18:47:02Z",20]]}]}]}`,
		},
		&Query{
			name:    "calculate derivative of percentile with unit default (2s) group by time",
			command: `SELECT derivative(percentile(temperature, 50)) from db0.rp0.air where time >= '2010-07-01 18:47:00' and time <= '2010-07-01 18:47:03' group by time(2s)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","derivative"],"values":[["2010-07-01T18:47:02Z",10]]}]}]}`,
		},
		&Query{
			name:    "calculate derivative of percentile with unit 4s group by time",
			command: `SELECT derivative(percentile(temperature, 50), 4s) from db0.rp0.air where time >= '2010-07-01 18:47:00' and time <= '2010-07-01 18:47:03' group by time(2s)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","derivative"],"values":[["2010-07-01T18:47:02Z",20]]}]}]}`,
		},
		&Query{
			name:    "calculate derivative of visibility divided by aggregate",
			command: `SELECT non_negative_derivative(mean(visibility), 1s) / last(pressure) * 100 AS usage FROM db0.rp0.air0 WHERE time >= '2010-07-01 18:47:00' AND time <= '2010-07-01 18:47:03' GROUP BY station, time(1s)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air0","tags":{"station":"XiaoMaiDao01"},"columns":["time","usage"],"values":[["2010-07-01T18:47:00Z",null],["2010-07-01T18:47:01Z",20],["2010-07-01T18:47:02Z",2],["2010-07-01T18:47:03Z",15]]},{"name":"air0","tags":{"station":"XiaoMaiDao02"},"columns":["time","usage"],"values":[["2010-07-01T18:47:00Z",null],["2010-07-01T18:47:01Z",5],["2010-07-01T18:47:02Z",39],["2010-07-01T18:47:03Z",17]]}]}]}`,
		},
	}...)

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


// Ensure the server can handle various group by time derivative queries.
func TestServer_Query_SelectGroupByTimeDerivativeWithFill(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig())
	defer s.Close()

	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: fmt.Sprintf(`air temperature=10 1278010020000000000
air temperature=20 1278010021000000000
`)},
	}

	test.addQueries([]*Query{
		&Query{
			name:    "calculate derivative of count with unit default (2s) group by time with fill 0",
			command: `SELECT derivative(count(temperature)) from db0.rp0.air where time >= '2010-07-01 18:47:00' and time <= '2010-07-01 18:47:03' group by time(2s) fill(0)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","derivative"],"values":[["2010-07-01T18:47:00Z",2],["2010-07-01T18:47:02Z",-2]]}]}]}`,
		},
		&Query{
			name:    "calculate derivative of count with unit 4s group by time with fill  0",
			command: `SELECT derivative(count(temperature), 4s) from db0.rp0.air where time >= '2010-07-01 18:47:00' and time <= '2010-07-01 18:47:03' group by time(2s) fill(0)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","derivative"],"values":[["2010-07-01T18:47:00Z",4],["2010-07-01T18:47:02Z",-4]]}]}]}`,
		},
		&Query{
			name:    "calculate derivative of count with unit default (2s) group by time with fill previous",
			command: `SELECT derivative(count(temperature)) from db0.rp0.air where time >= '2010-07-01 18:47:00' and time <= '2010-07-01 18:47:03' group by time(2s) fill(previous)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","derivative"],"values":[["2010-07-01T18:47:02Z",0]]}]}]}`,
		},
		&Query{
			name:    "calculate derivative of count with unit 4s group by time with fill previous",
			command: `SELECT derivative(count(temperature), 4s) from db0.rp0.air where time >= '2010-07-01 18:47:00' and time <= '2010-07-01 18:47:03' group by time(2s) fill(previous)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","derivative"],"values":[["2010-07-01T18:47:02Z",0]]}]}]}`,
		},
		&Query{
			name:    "calculate derivative of mean with unit default (2s) group by time with fill 0",
			command: `SELECT derivative(mean(temperature)) from db0.rp0.air where time >= '2010-07-01 18:47:00' and time <= '2010-07-01 18:47:03' group by time(2s) fill(0)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","derivative"],"values":[["2010-07-01T18:47:00Z",15],["2010-07-01T18:47:02Z",-15]]}]}]}`,
		},
		&Query{
			name:    "calculate derivative of mean with unit 4s group by time with fill 0",
			command: `SELECT derivative(mean(temperature), 4s) from db0.rp0.air where time >= '2010-07-01 18:47:00' and time <= '2010-07-01 18:47:03' group by time(2s) fill(0)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","derivative"],"values":[["2010-07-01T18:47:00Z",30],["2010-07-01T18:47:02Z",-30]]}]}]}`,
		},
		&Query{
			name:    "calculate derivative of mean with unit default (2s) group by time with fill previous",
			command: `SELECT derivative(mean(temperature)) from db0.rp0.air where time >= '2010-07-01 18:47:00' and time <= '2010-07-01 18:47:03' group by time(2s) fill(previous)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","derivative"],"values":[["2010-07-01T18:47:02Z",0]]}]}]}`,
		},
		&Query{
			name:    "calculate derivative of mean with unit 4s group by time with fill previous",
			command: `SELECT derivative(mean(temperature), 4s) from db0.rp0.air where time >= '2010-07-01 18:47:00' and time <= '2010-07-01 18:47:03' group by time(2s) fill(previous)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","derivative"],"values":[["2010-07-01T18:47:02Z",0]]}]}]}`,
		},
		&Query{
			name:    "calculate derivative of median with unit default (2s) group by time with fill 0",
			command: `SELECT derivative(median(temperature)) from db0.rp0.air where time >= '2010-07-01 18:47:00' and time <= '2010-07-01 18:47:03' group by time(2s) fill(0)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","derivative"],"values":[["2010-07-01T18:47:00Z",15],["2010-07-01T18:47:02Z",-15]]}]}]}`,
		},
		&Query{
			name:    "calculate derivative of median with unit 4s group by time with fill 0",
			command: `SELECT derivative(median(temperature), 4s) from db0.rp0.air where time >= '2010-07-01 18:47:00' and time <= '2010-07-01 18:47:03' group by time(2s) fill(0)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","derivative"],"values":[["2010-07-01T18:47:00Z",30],["2010-07-01T18:47:02Z",-30]]}]}]}`,
		},
		&Query{
			name:    "calculate derivative of median with unit default (2s) group by time with fill previous",
			command: `SELECT derivative(median(temperature)) from db0.rp0.air where time >= '2010-07-01 18:47:00' and time <= '2010-07-01 18:47:03' group by time(2s) fill(previous)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","derivative"],"values":[["2010-07-01T18:47:02Z",0]]}]}]}`,
		},
		&Query{
			name:    "calculate derivative of median with unit 4s group by time with fill previous",
			command: `SELECT derivative(median(temperature), 4s) from db0.rp0.air where time >= '2010-07-01 18:47:00' and time <= '2010-07-01 18:47:03' group by time(2s) fill(previous)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","derivative"],"values":[["2010-07-01T18:47:02Z",0]]}]}]}`,
		},
		&Query{
			name:    "calculate derivative of mode with unit default (2s) group by time with fill 0",
			command: `SELECT derivative(mode(temperature)) from db0.rp0.air where time >= '2010-07-01 18:47:00' and time <= '2010-07-01 18:47:03' group by time(2s) fill(0)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","derivative"],"values":[["2010-07-01T18:47:00Z",10],["2010-07-01T18:47:02Z",-10]]}]}]}`,
		},
		&Query{
			name:    "calculate derivative of mode with unit 4s group by time with fill 0",
			command: `SELECT derivative(mode(temperature), 4s) from db0.rp0.air where time >= '2010-07-01 18:47:00' and time <= '2010-07-01 18:47:03' group by time(2s) fill(0)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","derivative"],"values":[["2010-07-01T18:47:00Z",20],["2010-07-01T18:47:02Z",-20]]}]}]}`,
		},
		&Query{
			name:    "calculate derivative of mode with unit default (2s) group by time with fill previous",
			command: `SELECT derivative(mode(temperature)) from db0.rp0.air where time >= '2010-07-01 18:47:00' and time <= '2010-07-01 18:47:03' group by time(2s) fill(previous)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","derivative"],"values":[["2010-07-01T18:47:02Z",0]]}]}]}`,
		},
		&Query{
			name:    "calculate derivative of mode with unit 4s group by time with fill previous",
			command: `SELECT derivative(mode(temperature), 4s) from db0.rp0.air where time >= '2010-07-01 18:47:00' and time <= '2010-07-01 18:47:03' group by time(2s) fill(previous)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","derivative"],"values":[["2010-07-01T18:47:02Z",0]]}]}]}`,
		},
		&Query{
			name:    "calculate derivative of sum with unit default (2s) group by time with fill 0",
			command: `SELECT derivative(sum(temperature)) from db0.rp0.air where time >= '2010-07-01 18:47:00' and time <= '2010-07-01 18:47:03' group by time(2s) fill(0)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","derivative"],"values":[["2010-07-01T18:47:00Z",30],["2010-07-01T18:47:02Z",-30]]}]}]}`,
		},
		&Query{
			name:    "calculate derivative of sum with unit 4s group by time with fill 0",
			command: `SELECT derivative(sum(temperature), 4s) from db0.rp0.air where time >= '2010-07-01 18:47:00' and time <= '2010-07-01 18:47:03' group by time(2s) fill(0)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","derivative"],"values":[["2010-07-01T18:47:00Z",60],["2010-07-01T18:47:02Z",-60]]}]}]}`,
		},
		&Query{
			name:    "calculate derivative of sum with unit default (2s) group by time with fill previous",
			command: `SELECT derivative(sum(temperature)) from db0.rp0.air where time >= '2010-07-01 18:47:00' and time <= '2010-07-01 18:47:03' group by time(2s) fill(previous)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","derivative"],"values":[["2010-07-01T18:47:02Z",0]]}]}]}`,
		},
		&Query{
			name:    "calculate derivative of sum with unit 4s group by time with fill previous",
			command: `SELECT derivative(sum(temperature), 4s) from db0.rp0.air where time >= '2010-07-01 18:47:00' and time <= '2010-07-01 18:47:03' group by time(2s) fill(previous)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","derivative"],"values":[["2010-07-01T18:47:02Z",0]]}]}]}`,
		},
		&Query{
			name:    "calculate derivative of first with unit default (2s) group by time with fill 0",
			command: `SELECT derivative(first(temperature)) from db0.rp0.air where time >= '2010-07-01 18:47:00' and time <= '2010-07-01 18:47:03' group by time(2s) fill(0)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","derivative"],"values":[["2010-07-01T18:47:00Z",10],["2010-07-01T18:47:02Z",-10]]}]}]}`,
		},
		&Query{
			name:    "calculate derivative of first with unit 4s group by time with fill 0",
			command: `SELECT derivative(first(temperature), 4s) from db0.rp0.air where time >= '2010-07-01 18:47:00' and time <= '2010-07-01 18:47:03' group by time(2s) fill(0)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","derivative"],"values":[["2010-07-01T18:47:00Z",20],["2010-07-01T18:47:02Z",-20]]}]}]}`,
		},
		&Query{
			name:    "calculate derivative of first with unit default (2s) group by time with fill previous",
			command: `SELECT derivative(first(temperature)) from db0.rp0.air where time >= '2010-07-01 18:47:00' and time <= '2010-07-01 18:47:03' group by time(2s) fill(previous)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","derivative"],"values":[["2010-07-01T18:47:02Z",0]]}]}]}`,
		},
		&Query{
			name:    "calculate derivative of first with unit 4s group by time with fill previous",
			command: `SELECT derivative(first(temperature), 4s) from db0.rp0.air where time >= '2010-07-01 18:47:00' and time <= '2010-07-01 18:47:03' group by time(2s) fill(previous)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","derivative"],"values":[["2010-07-01T18:47:02Z",0]]}]}]}`,
		},
		&Query{
			name:    "calculate derivative of last with unit default (2s) group by time with fill 0",
			command: `SELECT derivative(last(temperature)) from db0.rp0.air where time >= '2010-07-01 18:47:00' and time <= '2010-07-01 18:47:03' group by time(2s) fill(0)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","derivative"],"values":[["2010-07-01T18:47:00Z",20],["2010-07-01T18:47:02Z",-20]]}]}]}`,
		},
		&Query{
			name:    "calculate derivative of last with unit 4s group by time with fill 0",
			command: `SELECT derivative(last(temperature), 4s) from db0.rp0.air where time >= '2010-07-01 18:47:00' and time <= '2010-07-01 18:47:03' group by time(2s) fill(0)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","derivative"],"values":[["2010-07-01T18:47:00Z",40],["2010-07-01T18:47:02Z",-40]]}]}]}`,
		},
		&Query{
			name:    "calculate derivative of last with unit default (2s) group by time with fill previous",
			command: `SELECT derivative(last(temperature)) from db0.rp0.air where time >= '2010-07-01 18:47:00' and time <= '2010-07-01 18:47:03' group by time(2s) fill(previous)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","derivative"],"values":[["2010-07-01T18:47:02Z",0]]}]}]}`,
		},
		&Query{
			name:    "calculate derivative of last with unit 4s group by time with fill previous",
			command: `SELECT derivative(last(temperature), 4s) from db0.rp0.air where time >= '2010-07-01 18:47:00' and time <= '2010-07-01 18:47:03' group by time(2s) fill(previous)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","derivative"],"values":[["2010-07-01T18:47:02Z",0]]}]}]}`,
		},
		&Query{
			name:    "calculate derivative of min with unit default (2s) group by time with fill 0",
			command: `SELECT derivative(min(temperature)) from db0.rp0.air where time >= '2010-07-01 18:47:00' and time <= '2010-07-01 18:47:03' group by time(2s) fill(0)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","derivative"],"values":[["2010-07-01T18:47:00Z",10],["2010-07-01T18:47:02Z",-10]]}]}]}`,
		},
		&Query{
			name:    "calculate derivative of min with unit 4s group by time with fill 0",
			command: `SELECT derivative(min(temperature), 4s) from db0.rp0.air where time >= '2010-07-01 18:47:00' and time <= '2010-07-01 18:47:03' group by time(2s) fill(0)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","derivative"],"values":[["2010-07-01T18:47:00Z",20],["2010-07-01T18:47:02Z",-20]]}]}]}`,
		},
		&Query{
			name:    "calculate derivative of min with unit default (2s) group by time with fill previous",
			command: `SELECT derivative(min(temperature)) from db0.rp0.air where time >= '2010-07-01 18:47:00' and time <= '2010-07-01 18:47:03' group by time(2s) fill(previous)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","derivative"],"values":[["2010-07-01T18:47:02Z",0]]}]}]}`,
		},
		&Query{
			name:    "calculate derivative of min with unit 4s group by time with fill previous",
			command: `SELECT derivative(min(temperature), 4s) from db0.rp0.air where time >= '2010-07-01 18:47:00' and time <= '2010-07-01 18:47:03' group by time(2s) fill(previous)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","derivative"],"values":[["2010-07-01T18:47:02Z",0]]}]}]}`,
		},
		&Query{
			name:    "calculate derivative of max with unit default (2s) group by time with fill 0",
			command: `SELECT derivative(max(temperature)) from db0.rp0.air where time >= '2010-07-01 18:47:00' and time <= '2010-07-01 18:47:03' group by time(2s) fill(0)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","derivative"],"values":[["2010-07-01T18:47:00Z",20],["2010-07-01T18:47:02Z",-20]]}]}]}`,
		},
		&Query{
			name:    "calculate derivative of max with unit 4s group by time with fill 0",
			command: `SELECT derivative(max(temperature), 4s) from db0.rp0.air where time >= '2010-07-01 18:47:00' and time <= '2010-07-01 18:47:03' group by time(2s) fill(0)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","derivative"],"values":[["2010-07-01T18:47:00Z",40],["2010-07-01T18:47:02Z",-40]]}]}]}`,
		},
		&Query{
			name:    "calculate derivative of max with unit default (2s) group by time with fill previous",
			command: `SELECT derivative(max(temperature)) from db0.rp0.air where time >= '2010-07-01 18:47:00' and time <= '2010-07-01 18:47:03' group by time(2s) fill(previous)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","derivative"],"values":[["2010-07-01T18:47:02Z",0]]}]}]}`,
		},
		&Query{
			name:    "calculate derivative of max with unit 4s group by time with fill previous",
			command: `SELECT derivative(max(temperature), 4s) from db0.rp0.air where time >= '2010-07-01 18:47:00' and time <= '2010-07-01 18:47:03' group by time(2s) fill(previous)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","derivative"],"values":[["2010-07-01T18:47:02Z",0]]}]}]}`,
		},
		&Query{
			name:    "calculate derivative of percentile with unit default (2s) group by time with fill 0",
			command: `SELECT derivative(percentile(temperature, 50)) from db0.rp0.air where time >= '2010-07-01 18:47:00' and time <= '2010-07-01 18:47:03' group by time(2s) fill(0)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","derivative"],"values":[["2010-07-01T18:47:00Z",10],["2010-07-01T18:47:02Z",-10]]}]}]}`,
		},
		&Query{
			name:    "calculate derivative of percentile with unit 4s group by time with fill 0",
			command: `SELECT derivative(percentile(temperature, 50), 4s) from db0.rp0.air where time >= '2010-07-01 18:47:00' and time <= '2010-07-01 18:47:03' group by time(2s) fill(0)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","derivative"],"values":[["2010-07-01T18:47:00Z",20],["2010-07-01T18:47:02Z",-20]]}]}]}`,
		},
		&Query{
			name:    "calculate derivative of percentile with unit default (2s) group by time with fill previous",
			command: `SELECT derivative(percentile(temperature, 50)) from db0.rp0.air where time >= '2010-07-01 18:47:00' and time <= '2010-07-01 18:47:03' group by time(2s) fill(previous)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","derivative"],"values":[["2010-07-01T18:47:02Z",0]]}]}]}`,
		},
		&Query{
			name:    "calculate derivative of percentile with unit 4s group by time with fill previous",
			command: `SELECT derivative(percentile(temperature, 50), 4s) from db0.rp0.air where time >= '2010-07-01 18:47:00' and time <= '2010-07-01 18:47:03' group by time(2s) fill(previous)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","derivative"],"values":[["2010-07-01T18:47:02Z",0]]}]}]}`,
		},
	}...)

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

// Ensure the server can handle various group by time difference queries.
func TestServer_Query_SelectGroupByTimeDifference(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig())
	defer s.Close()

	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: fmt.Sprintf(`air temperature=10 1278010020000000000
air temperature=15 1278010021000000000
air temperature=20 1278010022000000000
air temperature=25 1278010023000000000
`)},
	}

	test.addQueries([]*Query{
		&Query{
			name:    "calculate difference of count",
			command: `SELECT difference(count(temperature)) from db0.rp0.air where time >= '2010-07-01 18:47:00' and time <= '2010-07-01 18:47:03' group by time(2s)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","difference"],"values":[["2010-07-01T18:47:00Z",2],["2010-07-01T18:47:02Z",0]]}]}]}`,
		},
		&Query{
			name:    "calculate difference of mean",
			command: `SELECT difference(mean(temperature)) from db0.rp0.air where time >= '2010-07-01 18:47:00' and time <= '2010-07-01 18:47:03' group by time(2s)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","difference"],"values":[["2010-07-01T18:47:02Z",10]]}]}]}`,
		},
		&Query{
			name:    "calculate difference of median",
			command: `SELECT difference(median(temperature)) from db0.rp0.air where time >= '2010-07-01 18:47:00' and time <= '2010-07-01 18:47:03' group by time(2s)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","difference"],"values":[["2010-07-01T18:47:02Z",10]]}]}]}`,
		},
		&Query{
			name:    "calculate difference of mode",
			command: `SELECT difference(mode(temperature)) from db0.rp0.air where time >= '2010-07-01 18:47:00' and time <= '2010-07-01 18:47:03' group by time(2s)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","difference"],"values":[["2010-07-01T18:47:02Z",10]]}]}]}`,
		},
		&Query{
			name:    "calculate difference of sum",
			command: `SELECT difference(sum(temperature)) from db0.rp0.air where time >= '2010-07-01 18:47:00' and time <= '2010-07-01 18:47:03' group by time(2s)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","difference"],"values":[["2010-07-01T18:47:02Z",20]]}]}]}`,
		},
		&Query{
			name:    "calculate difference of first",
			command: `SELECT difference(first(temperature)) from db0.rp0.air where time >= '2010-07-01 18:47:00' and time <= '2010-07-01 18:47:03' group by time(2s)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","difference"],"values":[["2010-07-01T18:47:02Z",10]]}]}]}`,
		},
		&Query{
			name:    "calculate difference of last",
			command: `SELECT difference(last(temperature)) from db0.rp0.air where time >= '2010-07-01 18:47:00' and time <= '2010-07-01 18:47:03' group by time(2s)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","difference"],"values":[["2010-07-01T18:47:02Z",10]]}]}]}`,
		},
		&Query{
			name:    "calculate difference of min",
			command: `SELECT difference(min(temperature)) from db0.rp0.air where time >= '2010-07-01 18:47:00' and time <= '2010-07-01 18:47:03' group by time(2s)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","difference"],"values":[["2010-07-01T18:47:02Z",10]]}]}]}`,
		},
		&Query{
			name:    "calculate difference of max",
			command: `SELECT difference(max(temperature)) from db0.rp0.air where time >= '2010-07-01 18:47:00' and time <= '2010-07-01 18:47:03' group by time(2s)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","difference"],"values":[["2010-07-01T18:47:02Z",10]]}]}]}`,
		},
		&Query{
			name:    "calculate difference of percentile",
			command: `SELECT difference(percentile(temperature, 50)) from db0.rp0.air where time >= '2010-07-01 18:47:00' and time <= '2010-07-01 18:47:03' group by time(2s)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","difference"],"values":[["2010-07-01T18:47:02Z",10]]}]}]}`,
		},
	}...)

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

// Ensure the server can handle various group by time difference queries with fill.
func TestServer_Query_SelectGroupByTimeDifferenceWithFill(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig())
	defer s.Close()

	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: fmt.Sprintf(`air temperature=10 1278010020000000000
air temperature=20 1278010021000000000
`)},
	}

	test.addQueries([]*Query{
		&Query{
			name:    "calculate difference of count with fill 0",
			command: `SELECT difference(count(temperature)) from db0.rp0.air where time >= '2010-07-01 18:47:00' and time <= '2010-07-01 18:47:03' group by time(2s) fill(0)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","difference"],"values":[["2010-07-01T18:47:00Z",2],["2010-07-01T18:47:02Z",-2]]}]}]}`,
		},
		&Query{
			name:    "calculate difference of count with fill previous",
			command: `SELECT difference(count(temperature)) from db0.rp0.air where time >= '2010-07-01 18:47:00' and time <= '2010-07-01 18:47:03' group by time(2s) fill(previous)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","difference"],"values":[["2010-07-01T18:47:02Z",0]]}]}]}`,
		},
		&Query{
			name:    "calculate difference of mean with fill 0",
			command: `SELECT difference(mean(temperature)) from db0.rp0.air where time >= '2010-07-01 18:47:00' and time <= '2010-07-01 18:47:03' group by time(2s) fill(0)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","difference"],"values":[["2010-07-01T18:47:00Z",15],["2010-07-01T18:47:02Z",-15]]}]}]}`,
		},
		&Query{
			name:    "calculate difference of mean with fill previous",
			command: `SELECT difference(mean(temperature)) from db0.rp0.air where time >= '2010-07-01 18:47:00' and time <= '2010-07-01 18:47:03' group by time(2s) fill(previous)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","difference"],"values":[["2010-07-01T18:47:02Z",0]]}]}]}`,
		},
		&Query{
			name:    "calculate difference of median with fill 0",
			command: `SELECT difference(median(temperature)) from db0.rp0.air where time >= '2010-07-01 18:47:00' and time <= '2010-07-01 18:47:03' group by time(2s) fill(0)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","difference"],"values":[["2010-07-01T18:47:00Z",15],["2010-07-01T18:47:02Z",-15]]}]}]}`,
		},
		&Query{
			name:    "calculate difference of median with fill previous",
			command: `SELECT difference(median(temperature)) from db0.rp0.air where time >= '2010-07-01 18:47:00' and time <= '2010-07-01 18:47:03' group by time(2s) fill(previous)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","difference"],"values":[["2010-07-01T18:47:02Z",0]]}]}]}`,
		},
		&Query{
			name:    "calculate difference of mode with fill 0",
			command: `SELECT difference(mode(temperature)) from db0.rp0.air where time >= '2010-07-01 18:47:00' and time <= '2010-07-01 18:47:03' group by time(2s) fill(0)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","difference"],"values":[["2010-07-01T18:47:00Z",10],["2010-07-01T18:47:02Z",-10]]}]}]}`,
		},
		&Query{
			name:    "calculate difference of mode with fill previous",
			command: `SELECT difference(mode(temperature)) from db0.rp0.air where time >= '2010-07-01 18:47:00' and time <= '2010-07-01 18:47:03' group by time(2s) fill(previous)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","difference"],"values":[["2010-07-01T18:47:02Z",0]]}]}]}`,
		},
		&Query{
			name:    "calculate difference of sum with fill 0",
			command: `SELECT difference(sum(temperature)) from db0.rp0.air where time >= '2010-07-01 18:47:00' and time <= '2010-07-01 18:47:03' group by time(2s) fill(0)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","difference"],"values":[["2010-07-01T18:47:00Z",30],["2010-07-01T18:47:02Z",-30]]}]}]}`,
		},
		&Query{
			name:    "calculate difference of sum with fill previous",
			command: `SELECT difference(sum(temperature)) from db0.rp0.air where time >= '2010-07-01 18:47:00' and time <= '2010-07-01 18:47:03' group by time(2s) fill(previous)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","difference"],"values":[["2010-07-01T18:47:02Z",0]]}]}]}`,
		},
		&Query{
			name:    "calculate difference of first with fill 0",
			command: `SELECT difference(first(temperature)) from db0.rp0.air where time >= '2010-07-01 18:47:00' and time <= '2010-07-01 18:47:03' group by time(2s) fill(0)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","difference"],"values":[["2010-07-01T18:47:00Z",10],["2010-07-01T18:47:02Z",-10]]}]}]}`,
		},
		&Query{
			name:    "calculate difference of first with fill previous",
			command: `SELECT difference(first(temperature)) from db0.rp0.air where time >= '2010-07-01 18:47:00' and time <= '2010-07-01 18:47:03' group by time(2s) fill(previous)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","difference"],"values":[["2010-07-01T18:47:02Z",0]]}]}]}`,
		},
		&Query{
			name:    "calculate difference of last with fill 0",
			command: `SELECT difference(last(temperature)) from db0.rp0.air where time >= '2010-07-01 18:47:00' and time <= '2010-07-01 18:47:03' group by time(2s) fill(0)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","difference"],"values":[["2010-07-01T18:47:00Z",20],["2010-07-01T18:47:02Z",-20]]}]}]}`,
		},
		&Query{
			name:    "calculate difference of last with fill previous",
			command: `SELECT difference(last(temperature)) from db0.rp0.air where time >= '2010-07-01 18:47:00' and time <= '2010-07-01 18:47:03' group by time(2s) fill(previous)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","difference"],"values":[["2010-07-01T18:47:02Z",0]]}]}]}`,
		},
		&Query{
			name:    "calculate difference of min with fill 0",
			command: `SELECT difference(min(temperature)) from db0.rp0.air where time >= '2010-07-01 18:47:00' and time <= '2010-07-01 18:47:03' group by time(2s) fill(0)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","difference"],"values":[["2010-07-01T18:47:00Z",10],["2010-07-01T18:47:02Z",-10]]}]}]}`,
		},
		&Query{
			name:    "calculate difference of min with fill previous",
			command: `SELECT difference(min(temperature)) from db0.rp0.air where time >= '2010-07-01 18:47:00' and time <= '2010-07-01 18:47:03' group by time(2s) fill(previous)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","difference"],"values":[["2010-07-01T18:47:02Z",0]]}]}]}`,
		},
		&Query{
			name:    "calculate difference of max with fill 0",
			command: `SELECT difference(max(temperature)) from db0.rp0.air where time >= '2010-07-01 18:47:00' and time <= '2010-07-01 18:47:03' group by time(2s) fill(0)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","difference"],"values":[["2010-07-01T18:47:00Z",20],["2010-07-01T18:47:02Z",-20]]}]}]}`,
		},
		&Query{
			name:    "calculate difference of max with fill previous",
			command: `SELECT difference(max(temperature)) from db0.rp0.air where time >= '2010-07-01 18:47:00' and time <= '2010-07-01 18:47:03' group by time(2s) fill(previous)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","difference"],"values":[["2010-07-01T18:47:02Z",0]]}]}]}`,
		},
		&Query{
			name:    "calculate difference of percentile with fill 0",
			command: `SELECT difference(percentile(temperature, 50)) from db0.rp0.air where time >= '2010-07-01 18:47:00' and time <= '2010-07-01 18:47:03' group by time(2s) fill(0)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","difference"],"values":[["2010-07-01T18:47:00Z",10],["2010-07-01T18:47:02Z",-10]]}]}]}`,
		},
		&Query{
			name:    "calculate difference of percentile with fill previous",
			command: `SELECT difference(percentile(temperature, 50)) from db0.rp0.air where time >= '2010-07-01 18:47:00' and time <= '2010-07-01 18:47:03' group by time(2s) fill(previous)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","difference"],"values":[["2010-07-01T18:47:02Z",0]]}]}]}`,
		},
	}...)

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

// Ensure the server can handle various group by time moving average queries.
func TestServer_Query_SelectGroupByTimeMovingAverage(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig())
	defer s.Close()

	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: fmt.Sprintf(`air temperature=10 1278010020000000000
air temperature=15 1278010021000000000
air temperature=20 1278010022000000000
air temperature=25 1278010023000000000
air temperature=30 1278010024000000000
air temperature=35 1278010025000000000
`)},
	}

	test.addQueries([]*Query{
		&Query{
			name:    "calculate moving average of count",
			command: `SELECT moving_average(count(temperature), 2) from db0.rp0.air where time >= '2010-07-01 18:47:00' and time <= '2010-07-01 18:47:05' group by time(2s)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","moving_average"],"values":[["2010-07-01T18:47:00Z",1],["2010-07-01T18:47:02Z",2],["2010-07-01T18:47:04Z",2]]}]}]}`,
		},
		&Query{
			name:    "calculate moving average of mean",
			command: `SELECT moving_average(mean(temperature), 2) from db0.rp0.air where time >= '2010-07-01 18:47:00' and time <= '2010-07-01 18:47:05' group by time(2s)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","moving_average"],"values":[["2010-07-01T18:47:02Z",17.5],["2010-07-01T18:47:04Z",27.5]]}]}]}`,
		},
		&Query{
			name:    "calculate moving average of median",
			command: `SELECT moving_average(median(temperature), 2) from db0.rp0.air where time >= '2010-07-01 18:47:00' and time <= '2010-07-01 18:47:05' group by time(2s)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","moving_average"],"values":[["2010-07-01T18:47:02Z",17.5],["2010-07-01T18:47:04Z",27.5]]}]}]}`,
		},
		&Query{
			name:    "calculate moving average of mode",
			command: `SELECT moving_average(mode(temperature), 2) from db0.rp0.air where time >= '2010-07-01 18:47:00' and time <= '2010-07-01 18:47:05' group by time(2s)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","moving_average"],"values":[["2010-07-01T18:47:02Z",15],["2010-07-01T18:47:04Z",25]]}]}]}`,
		},
		&Query{
			name:    "calculate moving average of sum",
			command: `SELECT moving_average(sum(temperature), 2) from db0.rp0.air where time >= '2010-07-01 18:47:00' and time <= '2010-07-01 18:47:05' group by time(2s)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","moving_average"],"values":[["2010-07-01T18:47:02Z",35],["2010-07-01T18:47:04Z",55]]}]}]}`,
		},
		&Query{
			name:    "calculate moving average of first",
			command: `SELECT moving_average(first(temperature), 2) from db0.rp0.air where time >= '2010-07-01 18:47:00' and time <= '2010-07-01 18:47:05' group by time(2s)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","moving_average"],"values":[["2010-07-01T18:47:02Z",15],["2010-07-01T18:47:04Z",25]]}]}]}`,
		},
		&Query{
			name:    "calculate moving average of last",
			command: `SELECT moving_average(last(temperature), 2) from db0.rp0.air where time >= '2010-07-01 18:47:00' and time <= '2010-07-01 18:47:05' group by time(2s)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","moving_average"],"values":[["2010-07-01T18:47:02Z",20],["2010-07-01T18:47:04Z",30]]}]}]}`,
		},
		&Query{
			name:    "calculate moving average of min",
			command: `SELECT moving_average(min(temperature), 2) from db0.rp0.air where time >= '2010-07-01 18:47:00' and time <= '2010-07-01 18:47:05' group by time(2s)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","moving_average"],"values":[["2010-07-01T18:47:02Z",15],["2010-07-01T18:47:04Z",25]]}]}]}`,
		},
		&Query{
			name:    "calculate moving average of max",
			command: `SELECT moving_average(max(temperature), 2) from db0.rp0.air where time >= '2010-07-01 18:47:00' and time <= '2010-07-01 18:47:05' group by time(2s)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","moving_average"],"values":[["2010-07-01T18:47:02Z",20],["2010-07-01T18:47:04Z",30]]}]}]}`,
		},
		&Query{
			name:    "calculate moving average of percentile",
			command: `SELECT moving_average(percentile(temperature, 50), 2) from db0.rp0.air where time >= '2010-07-01 18:47:00' and time <= '2010-07-01 18:47:05' group by time(2s)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","moving_average"],"values":[["2010-07-01T18:47:02Z",15],["2010-07-01T18:47:04Z",25]]}]}]}`,
		},
	}...)

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

// Ensure the server can handle various group by time moving average queries.
func TestServer_Query_SelectGroupByTimeMovingAverageWithFill(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig())
	defer s.Close()

	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: fmt.Sprintf(`air temperature=10 1278010020000000000
air temperature=15 1278010021000000000
air temperature=30 1278010024000000000
air temperature=35 1278010025000000000
`)},
	}

	test.addQueries([]*Query{
		&Query{
			name:    "calculate moving average of count with fill 0",
			command: `SELECT moving_average(count(temperature), 2) from db0.rp0.air where time >= '2010-07-01 18:47:00' and time <= '2010-07-01 18:47:05' group by time(2s) fill(0)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","moving_average"],"values":[["2010-07-01T18:47:00Z",1],["2010-07-01T18:47:02Z",1],["2010-07-01T18:47:04Z",1]]}]}]}`,
		},
		&Query{
			name:    "calculate moving average of count with fill previous",
			command: `SELECT moving_average(count(temperature), 2) from db0.rp0.air where time >= '2010-07-01 18:47:00' and time <= '2010-07-01 18:47:05' group by time(2s) fill(previous)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","moving_average"],"values":[["2010-07-01T18:47:02Z",2],["2010-07-01T18:47:04Z",2]]}]}]}`,
		},
		&Query{
			name:    "calculate moving average of mean with fill 0",
			command: `SELECT moving_average(mean(temperature), 2) from db0.rp0.air where time >= '2010-07-01 18:47:00' and time <= '2010-07-01 18:47:05' group by time(2s) fill(0)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","moving_average"],"values":[["2010-07-01T18:47:00Z",6.25],["2010-07-01T18:47:02Z",6.25],["2010-07-01T18:47:04Z",16.25]]}]}]}`,
		},
		&Query{
			name:    "calculate moving average of mean with fill previous",
			command: `SELECT moving_average(mean(temperature), 2) from db0.rp0.air where time >= '2010-07-01 18:47:00' and time <= '2010-07-01 18:47:05' group by time(2s) fill(previous)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","moving_average"],"values":[["2010-07-01T18:47:02Z",12.5],["2010-07-01T18:47:04Z",22.5]]}]}]}`,
		},
		&Query{
			name:    "calculate moving average of median with fill 0",
			command: `SELECT moving_average(median(temperature), 2) from db0.rp0.air where time >= '2010-07-01 18:47:00' and time <= '2010-07-01 18:47:05' group by time(2s) fill(0)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","moving_average"],"values":[["2010-07-01T18:47:00Z",6.25],["2010-07-01T18:47:02Z",6.25],["2010-07-01T18:47:04Z",16.25]]}]}]}`,
		},
		&Query{
			name:    "calculate moving average of median with fill previous",
			command: `SELECT moving_average(median(temperature), 2) from db0.rp0.air where time >= '2010-07-01 18:47:00' and time <= '2010-07-01 18:47:05' group by time(2s) fill(previous)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","moving_average"],"values":[["2010-07-01T18:47:02Z",12.5],["2010-07-01T18:47:04Z",22.5]]}]}]}`,
		},
		&Query{
			name:    "calculate moving average of mode with fill 0",
			command: `SELECT moving_average(mode(temperature), 2) from db0.rp0.air where time >= '2010-07-01 18:47:00' and time <= '2010-07-01 18:47:05' group by time(2s) fill(0)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","moving_average"],"values":[["2010-07-01T18:47:00Z",5],["2010-07-01T18:47:02Z",5],["2010-07-01T18:47:04Z",15]]}]}]}`,
		},
		&Query{
			name:    "calculate moving average of mode with fill previous",
			command: `SELECT moving_average(mode(temperature), 2) from db0.rp0.air where time >= '2010-07-01 18:47:00' and time <= '2010-07-01 18:47:05' group by time(2s) fill(previous)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","moving_average"],"values":[["2010-07-01T18:47:02Z",10],["2010-07-01T18:47:04Z",20]]}]}]}`,
		},
		&Query{
			name:    "calculate moving average of sum with fill 0",
			command: `SELECT moving_average(sum(temperature), 2) from db0.rp0.air where time >= '2010-07-01 18:47:00' and time <= '2010-07-01 18:47:05' group by time(2s) fill(0)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","moving_average"],"values":[["2010-07-01T18:47:00Z",12.5],["2010-07-01T18:47:02Z",12.5],["2010-07-01T18:47:04Z",32.5]]}]}]}`,
		},
		&Query{
			name:    "calculate moving average of sum with fill previous",
			command: `SELECT moving_average(sum(temperature), 2) from db0.rp0.air where time >= '2010-07-01 18:47:00' and time <= '2010-07-01 18:47:05' group by time(2s) fill(previous)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","moving_average"],"values":[["2010-07-01T18:47:02Z",25],["2010-07-01T18:47:04Z",45]]}]}]}`,
		},
		&Query{
			name:    "calculate moving average of first with fill 0",
			command: `SELECT moving_average(first(temperature), 2) from db0.rp0.air where time >= '2010-07-01 18:47:00' and time <= '2010-07-01 18:47:05' group by time(2s) fill(0)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","moving_average"],"values":[["2010-07-01T18:47:00Z",5],["2010-07-01T18:47:02Z",5],["2010-07-01T18:47:04Z",15]]}]}]}`,
		},
		&Query{
			name:    "calculate moving average of first with fill previous",
			command: `SELECT moving_average(first(temperature), 2) from db0.rp0.air where time >= '2010-07-01 18:47:00' and time <= '2010-07-01 18:47:05' group by time(2s) fill(previous)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","moving_average"],"values":[["2010-07-01T18:47:02Z",10],["2010-07-01T18:47:04Z",20]]}]}]}`,
		},
		&Query{
			name:    "calculate moving average of last with fill 0",
			command: `SELECT moving_average(last(temperature), 2) from db0.rp0.air where time >= '2010-07-01 18:47:00' and time <= '2010-07-01 18:47:05' group by time(2s) fill(0)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","moving_average"],"values":[["2010-07-01T18:47:00Z",7.5],["2010-07-01T18:47:02Z",7.5],["2010-07-01T18:47:04Z",17.5]]}]}]}`,
		},
		&Query{
			name:    "calculate moving average of last with fill previous",
			command: `SELECT moving_average(last(temperature), 2) from db0.rp0.air where time >= '2010-07-01 18:47:00' and time <= '2010-07-01 18:47:05' group by time(2s) fill(previous)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","moving_average"],"values":[["2010-07-01T18:47:02Z",15],["2010-07-01T18:47:04Z",25]]}]}]}`,
		},
		&Query{
			name:    "calculate moving average of min with fill 0",
			command: `SELECT moving_average(min(temperature), 2) from db0.rp0.air where time >= '2010-07-01 18:47:00' and time <= '2010-07-01 18:47:05' group by time(2s) fill(0)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","moving_average"],"values":[["2010-07-01T18:47:00Z",5],["2010-07-01T18:47:02Z",5],["2010-07-01T18:47:04Z",15]]}]}]}`,
		},
		&Query{
			name:    "calculate moving average of min with fill previous",
			command: `SELECT moving_average(min(temperature), 2) from db0.rp0.air where time >= '2010-07-01 18:47:00' and time <= '2010-07-01 18:47:05' group by time(2s) fill(previous)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","moving_average"],"values":[["2010-07-01T18:47:02Z",10],["2010-07-01T18:47:04Z",20]]}]}]}`,
		},
		&Query{
			name:    "calculate moving average of max with fill 0",
			command: `SELECT moving_average(max(temperature), 2) from db0.rp0.air where time >= '2010-07-01 18:47:00' and time <= '2010-07-01 18:47:05' group by time(2s) fill(0)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","moving_average"],"values":[["2010-07-01T18:47:00Z",7.5],["2010-07-01T18:47:02Z",7.5],["2010-07-01T18:47:04Z",17.5]]}]}]}`,
		},
		&Query{
			name:    "calculate moving average of max with fill previous",
			command: `SELECT moving_average(max(temperature), 2) from db0.rp0.air where time >= '2010-07-01 18:47:00' and time <= '2010-07-01 18:47:05' group by time(2s) fill(previous)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","moving_average"],"values":[["2010-07-01T18:47:02Z",15],["2010-07-01T18:47:04Z",25]]}]}]}`,
		},
		&Query{
			name:    "calculate moving average of percentile with fill 0",
			command: `SELECT moving_average(percentile(temperature, 50), 2) from db0.rp0.air where time >= '2010-07-01 18:47:00' and time <= '2010-07-01 18:47:05' group by time(2s) fill(0)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","moving_average"],"values":[["2010-07-01T18:47:00Z",5],["2010-07-01T18:47:02Z",5],["2010-07-01T18:47:04Z",15]]}]}]}`,
		},
		&Query{
			name:    "calculate moving average of percentile with fill previous",
			command: `SELECT moving_average(percentile(temperature, 50), 2) from db0.rp0.air where time >= '2010-07-01 18:47:00' and time <= '2010-07-01 18:47:05' group by time(2s) fill(previous)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","moving_average"],"values":[["2010-07-01T18:47:02Z",10],["2010-07-01T18:47:04Z",20]]}]}]}`,
		},
	}...)

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

// Ensure the server can handle various group by time cumulative sum queries.
func TestServer_Query_SelectGroupByTimeCumulativeSum(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig())
	defer s.Close()

	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: fmt.Sprintf(`air temperature=10 1278010020000000000
air temperature=15 1278010021000000000
air temperature=20 1278010022000000000
air temperature=25 1278010023000000000
`)},
	}

	test.addQueries([]*Query{
		&Query{
			name:    "calculate cumulative sum of count",
			command: `SELECT cumulative_sum(count(temperature)) from db0.rp0.air where time >= '2010-07-01 18:47:00' and time <= '2010-07-01 18:47:03' group by time(2s)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","cumulative_sum"],"values":[["2010-07-01T18:47:00Z",2],["2010-07-01T18:47:02Z",4]]}]}]}`,
		},
		&Query{
			name:    "calculate cumulative sum of mean",
			command: `SELECT cumulative_sum(mean(temperature)) from db0.rp0.air where time >= '2010-07-01 18:47:00' and time <= '2010-07-01 18:47:03' group by time(2s)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","cumulative_sum"],"values":[["2010-07-01T18:47:00Z",12.5],["2010-07-01T18:47:02Z",35]]}]}]}`,
		},
		&Query{
			name:    "calculate cumulative sum of median",
			command: `SELECT cumulative_sum(median(temperature)) from db0.rp0.air where time >= '2010-07-01 18:47:00' and time <= '2010-07-01 18:47:03' group by time(2s)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","cumulative_sum"],"values":[["2010-07-01T18:47:00Z",12.5],["2010-07-01T18:47:02Z",35]]}]}]}`,
		},
		&Query{
			name:    "calculate cumulative sum of mode",
			command: `SELECT cumulative_sum(mode(temperature)) from db0.rp0.air where time >= '2010-07-01 18:47:00' and time <= '2010-07-01 18:47:03' group by time(2s)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","cumulative_sum"],"values":[["2010-07-01T18:47:00Z",10],["2010-07-01T18:47:02Z",30]]}]}]}`,
		},
		&Query{
			name:    "calculate cumulative sum of sum",
			command: `SELECT cumulative_sum(sum(temperature)) from db0.rp0.air where time >= '2010-07-01 18:47:00' and time <= '2010-07-01 18:47:03' group by time(2s)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","cumulative_sum"],"values":[["2010-07-01T18:47:00Z",25],["2010-07-01T18:47:02Z",70]]}]}]}`,
		},
		&Query{
			name:    "calculate cumulative sum of first",
			command: `SELECT cumulative_sum(first(temperature)) from db0.rp0.air where time >= '2010-07-01 18:47:00' and time <= '2010-07-01 18:47:03' group by time(2s)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","cumulative_sum"],"values":[["2010-07-01T18:47:00Z",10],["2010-07-01T18:47:02Z",30]]}]}]}`,
		},
		&Query{
			name:    "calculate cumulative sum of last",
			command: `SELECT cumulative_sum(last(temperature)) from db0.rp0.air where time >= '2010-07-01 18:47:00' and time <= '2010-07-01 18:47:03' group by time(2s)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","cumulative_sum"],"values":[["2010-07-01T18:47:00Z",15],["2010-07-01T18:47:02Z",40]]}]}]}`,
		},
		&Query{
			name:    "calculate cumulative sum of min",
			command: `SELECT cumulative_sum(min(temperature)) from db0.rp0.air where time >= '2010-07-01 18:47:00' and time <= '2010-07-01 18:47:03' group by time(2s)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","cumulative_sum"],"values":[["2010-07-01T18:47:00Z",10],["2010-07-01T18:47:02Z",30]]}]}]}`,
		},
		&Query{
			name:    "calculate cumulative sum of max",
			command: `SELECT cumulative_sum(max(temperature)) from db0.rp0.air where time >= '2010-07-01 18:47:00' and time <= '2010-07-01 18:47:03' group by time(2s)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","cumulative_sum"],"values":[["2010-07-01T18:47:00Z",15],["2010-07-01T18:47:02Z",40]]}]}]}`,
		},
		&Query{
			name:    "calculate cumulative sum of percentile",
			command: `SELECT cumulative_sum(percentile(temperature, 50)) from db0.rp0.air where time >= '2010-07-01 18:47:00' and time <= '2010-07-01 18:47:03' group by time(2s)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","cumulative_sum"],"values":[["2010-07-01T18:47:00Z",10],["2010-07-01T18:47:02Z",30]]}]}]}`,
		},
	}...)

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

// Ensure the server can handle various group by time cumulative sum queries with fill.
func TestServer_Query_SelectGroupByTimeCumulativeSumWithFill(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig())
	defer s.Close()

	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: fmt.Sprintf(`air temperature=10 1278010020000000000
air temperature=20 1278010021000000000
`)},
	}

	test.addQueries([]*Query{
		&Query{
			name:    "calculate cumulative sum of count with fill 0",
			command: `SELECT cumulative_sum(count(temperature)) from db0.rp0.air where time >= '2010-07-01 18:47:00' and time <= '2010-07-01 18:47:03' group by time(2s) fill(0)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","cumulative_sum"],"values":[["2010-07-01T18:47:00Z",2],["2010-07-01T18:47:02Z",2]]}]}]}`,
		},
		&Query{
			name:    "calculate cumulative sum of count with fill previous",
			command: `SELECT cumulative_sum(count(temperature)) from db0.rp0.air where time >= '2010-07-01 18:47:00' and time <= '2010-07-01 18:47:03' group by time(2s) fill(previous)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","cumulative_sum"],"values":[["2010-07-01T18:47:00Z",2],["2010-07-01T18:47:02Z",4]]}]}]}`,
		},
		&Query{
			name:    "calculate cumulative sum of mean with fill 0",
			command: `SELECT cumulative_sum(mean(temperature)) from db0.rp0.air where time >= '2010-07-01 18:47:00' and time <= '2010-07-01 18:47:03' group by time(2s) fill(0)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","cumulative_sum"],"values":[["2010-07-01T18:47:00Z",15],["2010-07-01T18:47:02Z",15]]}]}]}`,
		},
		&Query{
			name:    "calculate cumulative sum of mean with fill previous",
			command: `SELECT cumulative_sum(mean(temperature)) from db0.rp0.air where time >= '2010-07-01 18:47:00' and time <= '2010-07-01 18:47:03' group by time(2s) fill(previous)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","cumulative_sum"],"values":[["2010-07-01T18:47:00Z",15],["2010-07-01T18:47:02Z",30]]}]}]}`,
		},
		&Query{
			name:    "calculate cumulative sum of median with fill 0",
			command: `SELECT cumulative_sum(median(temperature)) from db0.rp0.air where time >= '2010-07-01 18:47:00' and time <= '2010-07-01 18:47:03' group by time(2s) fill(0)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","cumulative_sum"],"values":[["2010-07-01T18:47:00Z",15],["2010-07-01T18:47:02Z",15]]}]}]}`,
		},
		&Query{
			name:    "calculate cumulative sum of median with fill previous",
			command: `SELECT cumulative_sum(median(temperature)) from db0.rp0.air where time >= '2010-07-01 18:47:00' and time <= '2010-07-01 18:47:03' group by time(2s) fill(previous)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","cumulative_sum"],"values":[["2010-07-01T18:47:00Z",15],["2010-07-01T18:47:02Z",30]]}]}]}`,
		},
		&Query{
			name:    "calculate cumulative sum of mode with fill 0",
			command: `SELECT cumulative_sum(mode(temperature)) from db0.rp0.air where time >= '2010-07-01 18:47:00' and time <= '2010-07-01 18:47:03' group by time(2s) fill(0)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","cumulative_sum"],"values":[["2010-07-01T18:47:00Z",10],["2010-07-01T18:47:02Z",10]]}]}]}`,
		},
		&Query{
			name:    "calculate cumulative sum of mode with fill previous",
			command: `SELECT cumulative_sum(mode(temperature)) from db0.rp0.air where time >= '2010-07-01 18:47:00' and time <= '2010-07-01 18:47:03' group by time(2s) fill(previous)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","cumulative_sum"],"values":[["2010-07-01T18:47:00Z",10],["2010-07-01T18:47:02Z",20]]}]}]}`,
		},
		&Query{
			name:    "calculate cumulative sum of sum with fill 0",
			command: `SELECT cumulative_sum(sum(temperature)) from db0.rp0.air where time >= '2010-07-01 18:47:00' and time <= '2010-07-01 18:47:03' group by time(2s) fill(0)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","cumulative_sum"],"values":[["2010-07-01T18:47:00Z",30],["2010-07-01T18:47:02Z",30]]}]}]}`,
		},
		&Query{
			name:    "calculate cumulative sum of sum with fill previous",
			command: `SELECT cumulative_sum(sum(temperature)) from db0.rp0.air where time >= '2010-07-01 18:47:00' and time <= '2010-07-01 18:47:03' group by time(2s) fill(previous)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","cumulative_sum"],"values":[["2010-07-01T18:47:00Z",30],["2010-07-01T18:47:02Z",60]]}]}]}`,
		},
		&Query{
			name:    "calculate cumulative sum of first with fill 0",
			command: `SELECT cumulative_sum(first(temperature)) from db0.rp0.air where time >= '2010-07-01 18:47:00' and time <= '2010-07-01 18:47:03' group by time(2s) fill(0)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","cumulative_sum"],"values":[["2010-07-01T18:47:00Z",10],["2010-07-01T18:47:02Z",10]]}]}]}`,
		},
		&Query{
			name:    "calculate cumulative sum of first with fill previous",
			command: `SELECT cumulative_sum(first(temperature)) from db0.rp0.air where time >= '2010-07-01 18:47:00' and time <= '2010-07-01 18:47:03' group by time(2s) fill(previous)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","cumulative_sum"],"values":[["2010-07-01T18:47:00Z",10],["2010-07-01T18:47:02Z",20]]}]}]}`,
		},
		&Query{
			name:    "calculate cumulative sum of last with fill 0",
			command: `SELECT cumulative_sum(last(temperature)) from db0.rp0.air where time >= '2010-07-01 18:47:00' and time <= '2010-07-01 18:47:03' group by time(2s) fill(0)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","cumulative_sum"],"values":[["2010-07-01T18:47:00Z",20],["2010-07-01T18:47:02Z",20]]}]}]}`,
		},
		&Query{
			name:    "calculate cumulative sum of last with fill previous",
			command: `SELECT cumulative_sum(last(temperature)) from db0.rp0.air where time >= '2010-07-01 18:47:00' and time <= '2010-07-01 18:47:03' group by time(2s) fill(previous)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","cumulative_sum"],"values":[["2010-07-01T18:47:00Z",20],["2010-07-01T18:47:02Z",40]]}]}]}`,
		},
		&Query{
			name:    "calculate cumulative sum of min with fill 0",
			command: `SELECT cumulative_sum(min(temperature)) from db0.rp0.air where time >= '2010-07-01 18:47:00' and time <= '2010-07-01 18:47:03' group by time(2s) fill(0)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","cumulative_sum"],"values":[["2010-07-01T18:47:00Z",10],["2010-07-01T18:47:02Z",10]]}]}]}`,
		},
		&Query{
			name:    "calculate cumulative sum of min with fill previous",
			command: `SELECT cumulative_sum(min(temperature)) from db0.rp0.air where time >= '2010-07-01 18:47:00' and time <= '2010-07-01 18:47:03' group by time(2s) fill(previous)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","cumulative_sum"],"values":[["2010-07-01T18:47:00Z",10],["2010-07-01T18:47:02Z",20]]}]}]}`,
		},
		&Query{
			name:    "calculate cumulative sum of max with fill 0",
			command: `SELECT cumulative_sum(max(temperature)) from db0.rp0.air where time >= '2010-07-01 18:47:00' and time <= '2010-07-01 18:47:03' group by time(2s) fill(0)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","cumulative_sum"],"values":[["2010-07-01T18:47:00Z",20],["2010-07-01T18:47:02Z",20]]}]}]}`,
		},
		&Query{
			name:    "calculate cumulative sum of max with fill previous",
			command: `SELECT cumulative_sum(max(temperature)) from db0.rp0.air where time >= '2010-07-01 18:47:00' and time <= '2010-07-01 18:47:03' group by time(2s) fill(previous)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","cumulative_sum"],"values":[["2010-07-01T18:47:00Z",20],["2010-07-01T18:47:02Z",40]]}]}]}`,
		},
		&Query{
			name:    "calculate cumulative sum of percentile with fill 0",
			command: `SELECT cumulative_sum(percentile(temperature, 50)) from db0.rp0.air where time >= '2010-07-01 18:47:00' and time <= '2010-07-01 18:47:03' group by time(2s) fill(0)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","cumulative_sum"],"values":[["2010-07-01T18:47:00Z",10],["2010-07-01T18:47:02Z",10]]}]}]}`,
		},
		&Query{
			name:    "calculate cumulative sum of percentile with fill previous",
			command: `SELECT cumulative_sum(percentile(temperature, 50)) from db0.rp0.air where time >= '2010-07-01 18:47:00' and time <= '2010-07-01 18:47:03' group by time(2s) fill(previous)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","cumulative_sum"],"values":[["2010-07-01T18:47:00Z",10],["2010-07-01T18:47:02Z",20]]}]}]}`,
		},
	}...)

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

func TestServer_Query_CumulativeCount(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig())
	defer s.Close()

	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: fmt.Sprintf(`events signup=t 1005832000
events signup=t 1048283000
events signup=t 1784832000
events signup=t 2000000000
events signup=t 3084890000
events signup=t 3838400000
`)},
	}

	test.addQueries([]*Query{
		&Query{
			name:    "cumulative count",
			command: `SELECT cumulative_sum(count(signup)) from db0.rp0.events where time >= 1s and time < 4s group by time(1s)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"events","columns":["time","cumulative_sum"],"values":[["1970-01-01T00:00:01Z",3],["1970-01-01T00:00:02Z",4],["1970-01-01T00:00:03Z",6]]}]}]}`,
		},
	}...)

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

func TestServer_Query_SelectGroupByTime_MultipleAggregates(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig())
	defer s.Close()

	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: fmt.Sprintf(`test,t=a x=1i 1000000000
test,t=b y=1i 1000000000
test,t=a x=2i 2000000000
test,t=b y=2i 2000000000
test,t=a x=3i 3000000000
test,t=b y=3i 3000000000
`)},
	}

	test.addQueries([]*Query{
		&Query{
			name:    "two aggregates with a group by host",
			command: `SELECT mean(x) as x, mean(y) as y from db0.rp0.test where time >= 1s and time < 4s group by t, time(1s)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"test","tags":{"t":"a"},"columns":["time","x","y"],"values":[["1970-01-01T00:00:01Z",1,null],["1970-01-01T00:00:02Z",2,null],["1970-01-01T00:00:03Z",3,null]]},{"name":"test","tags":{"t":"b"},"columns":["time","x","y"],"values":[["1970-01-01T00:00:01Z",null,1],["1970-01-01T00:00:02Z",null,2],["1970-01-01T00:00:03Z",null,3]]}]}]}`,
		},
	}...)

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

func TestServer_Query_MathWithFill(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig())
	defer s.Close()

	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: fmt.Sprintf(`air temperature=15 1278010020000000000
`)},
	}

	test.addQueries([]*Query{
		&Query{
			name:    "multiplication with fill previous",
			command: `SELECT 4*mean(temperature) FROM db0.rp0.air WHERE time >= '2010-07-01 18:47:00' AND time < '2010-07-01 18:48:30' GROUP BY time(30s) FILL(previous)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","mean"],"values":[["2010-07-01T18:47:00Z",60],["2010-07-01T18:47:30Z",60],["2010-07-01T18:48:00Z",60]]}]}]}`,
		},
		&Query{
			name:    "multiplication of mode temperature with fill previous",
			command: `SELECT 4*mode(temperature) FROM db0.rp0.air WHERE time >= '2010-07-01 18:47:00' AND time < '2010-07-01 18:48:30' GROUP BY time(30s) FILL(previous)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","mode"],"values":[["2010-07-01T18:47:00Z",60],["2010-07-01T18:47:30Z",60],["2010-07-01T18:48:00Z",60]]}]}]}`,
		},
	}...)

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

// mergeMany ensures that when merging many series together and some of them have a different number
// of points than others in a group by interval the results are correct
func TestServer_Query_MergeMany(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig())
	defer s.Close()

	// set infinite retention policy as we are inserting data in the past and don't want retention policy enforcement to make this test racy
	if err := s.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("rp0", 1, 0), true); err != nil {
		t.Fatal(err)
	}

	test := NewTest("db0", "rp0")

	writes := []string{}
	for i := 1; i < 11; i++ {
		for j := 1; j < 5+i%3; j++ {
			data := fmt.Sprintf(`air,station=XiaoMaiDao_%d temperature=22 %d`, i, time.Unix(int64(j), int64(0)).UTC().UnixNano())
			writes = append(writes, data)
		}
	}
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}

	test.addQueries([]*Query{
		&Query{
			name:    "GROUP by time",
			command: `SELECT count(temperature) FROM db0.rp0.air WHERE time >= '1970-01-01T00:00:01Z' AND time <= '1970-01-01T00:00:06Z' GROUP BY time(1s)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","count"],"values":[["1970-01-01T00:00:01Z",10],["1970-01-01T00:00:02Z",10],["1970-01-01T00:00:03Z",10],["1970-01-01T00:00:04Z",10],["1970-01-01T00:00:05Z",7],["1970-01-01T00:00:06Z",3]]}]}]}`,
		},
		&Query{
			name:    "GROUP by field",
			command: `SELECT count(temperature) FROM db0.rp0.air group by temperature`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","tags":{"temperature":""},"columns":["time","count"],"values":[["1970-01-01T00:00:00Z",50]]}]}]}`,
		},
	}...)

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

func TestServer_Query_SLimitAndSOffset(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig())
	defer s.Close()

	// set infinite retention policy as we are inserting data in the past and don't want retention policy enforcement to make this test racy
	if err := s.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("rp0", 1, 0), true); err != nil {
		t.Fatal(err)
	}

	test := NewTest("db0", "rp0")

	writes := []string{}
	for i := 1; i < 10; i++ {
		data := fmt.Sprintf(`air,region=us-east,station=XiaoMaiDao-%d temperature=%d %d`, i, i, time.Unix(int64(i), int64(0)).UnixNano())
		writes = append(writes, data)
	}
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}

	test.addQueries([]*Query{
		&Query{
			name:    "SLIMIT 2 SOFFSET 1",
			command: `SELECT count(temperature) FROM db0.rp0.air GROUP BY * SLIMIT 2 SOFFSET 1`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","tags":{"region":"us-east","station":"XiaoMaiDao-2"},"columns":["time","count"],"values":[["1970-01-01T00:00:00Z",1]]},{"name":"air","tags":{"region":"us-east","station":"XiaoMaiDao-3"},"columns":["time","count"],"values":[["1970-01-01T00:00:00Z",1]]}]}]}`,
		},
		&Query{
			name:    "SLIMIT 2 SOFFSET 3",
			command: `SELECT count(temperature) FROM db0.rp0.air GROUP BY * SLIMIT 2 SOFFSET 3`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","tags":{"region":"us-east","station":"XiaoMaiDao-4"},"columns":["time","count"],"values":[["1970-01-01T00:00:00Z",1]]},{"name":"air","tags":{"region":"us-east","station":"XiaoMaiDao-5"},"columns":["time","count"],"values":[["1970-01-01T00:00:00Z",1]]}]}]}`,
		},
		&Query{
			name:    "SLIMIT 3 SOFFSET 8",
			command: `SELECT count(temperature) FROM db0.rp0.air GROUP BY * SLIMIT 3 SOFFSET 8`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","tags":{"region":"us-east","station":"XiaoMaiDao-9"},"columns":["time","count"],"values":[["1970-01-01T00:00:00Z",1]]}]}]}`,
		},
	}...)

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

func TestServer_Query_Regex(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig())
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("rp0", 1, 0), true); err != nil {
		t.Fatal(err)
	}

	writes := []string{
		fmt.Sprintf(`air1,station=XiaoMaiDao01 temperature=10 %d`, mustParseTime(time.RFC3339Nano, "2015-02-28T01:03:36.703820946Z").UnixNano()),
		fmt.Sprintf(`air2,station=XiaoMaiDao01 temperature=20 %d`, mustParseTime(time.RFC3339Nano, "2015-02-28T01:03:36.703820946Z").UnixNano()),
		fmt.Sprintf(`air3,station=XiaoMaiDao01 temperature=30 %d`, mustParseTime(time.RFC3339Nano, "2015-02-28T01:03:36.703820946Z").UnixNano()),
	}

	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}

	test.addQueries([]*Query{
		&Query{
			name:    "default db and rp",
			command: `SELECT * FROM /air[13]/`,
			params:  url.Values{"db": []string{"db0"}},
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air1","columns":["time","station","temperature"],"values":[["2015-02-28T01:03:36.703820946Z","XiaoMaiDao01",10]]},{"name":"air3","columns":["time","station","temperature"],"values":[["2015-02-28T01:03:36.703820946Z","XiaoMaiDao01",30]]}]}]}`,
		},
		&Query{
			name:    "default db and rp with GROUP BY *",
			command: `SELECT * FROM /air[13]/ GROUP BY *`,
			params:  url.Values{"db": []string{"db0"}},
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air1","tags":{"station":"XiaoMaiDao01"},"columns":["time","temperature"],"values":[["2015-02-28T01:03:36.703820946Z",10]]},{"name":"air3","tags":{"station":"XiaoMaiDao01"},"columns":["time","temperature"],"values":[["2015-02-28T01:03:36.703820946Z",30]]}]}]}`,
		},
		&Query{
			name:    "specifying db and rp",
			command: `SELECT * FROM db0.rp0./air[13]/ GROUP BY *`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air1","tags":{"station":"XiaoMaiDao01"},"columns":["time","temperature"],"values":[["2015-02-28T01:03:36.703820946Z",10]]},{"name":"air3","tags":{"station":"XiaoMaiDao01"},"columns":["time","temperature"],"values":[["2015-02-28T01:03:36.703820946Z",30]]}]}]}`,
		},
		&Query{
			name:    "default db and specified rp",
			command: `SELECT * FROM rp0./air[13]/ GROUP BY *`,
			params:  url.Values{"db": []string{"db0"}},
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air1","tags":{"station":"XiaoMaiDao01"},"columns":["time","temperature"],"values":[["2015-02-28T01:03:36.703820946Z",10]]},{"name":"air3","tags":{"station":"XiaoMaiDao01"},"columns":["time","temperature"],"values":[["2015-02-28T01:03:36.703820946Z",30]]}]}]}`,
		},
		&Query{
			name:    "specified db and default rp",
			command: `SELECT * FROM db0../air[13]/ GROUP BY *`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air1","tags":{"station":"XiaoMaiDao01"},"columns":["time","temperature"],"values":[["2015-02-28T01:03:36.703820946Z",10]]},{"name":"air3","tags":{"station":"XiaoMaiDao01"},"columns":["time","temperature"],"values":[["2015-02-28T01:03:36.703820946Z",30]]}]}]}`,
		},
		&Query{
			name:    "map field type with a regex source",
			command: `SELECT temperature FROM /air[13]/`,
			params:  url.Values{"db": []string{"db0"}},
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air1","columns":["time","temperature"],"values":[["2015-02-28T01:03:36.703820946Z",10]]},{"name":"air3","columns":["time","temperature"],"values":[["2015-02-28T01:03:36.703820946Z",30]]}]}]}`,
		},
	}...)

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

func TestServer_Query_Aggregates_Int(t *testing.T) {
	t.Parallel()
	s := OpenDefaultServer(NewConfig())
	defer s.Close()

	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: strings.Join([]string{
			fmt.Sprintf(`int temperature=45 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00Z").UnixNano()),
		}, "\n")},
	}

	test.addQueries([]*Query{
		// int64
		&Query{
			name:    "stddev with just one point - int",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT STDDEV(temperature) FROM int`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"int","columns":["time","stddev"],"values":[["1970-01-01T00:00:00Z",null]]}]}]}`,
		},
	}...)

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

func TestServer_Query_Aggregates_IntMax(t *testing.T) {
	t.Parallel()
	s := OpenDefaultServer(NewConfig())
	defer s.Close()

	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: strings.Join([]string{
			fmt.Sprintf(`intmax value=%s %d`, maxInt64(), mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00Z").UnixNano()),
			fmt.Sprintf(`intmax value=%s %d`, maxInt64(), mustParseTime(time.RFC3339Nano, "2000-01-01T01:00:00Z").UnixNano()),
		}, "\n")},
	}

	test.addQueries([]*Query{
		&Query{
			name:    "large mean and stddev - int",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT MEAN(value), STDDEV(value) FROM intmax`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"intmax","columns":["time","mean","stddev"],"values":[["1970-01-01T00:00:00Z",` + maxInt64() + `,0]]}]}]}`,
		},
	}...)

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

func TestServer_Query_Aggregates_IntMany(t *testing.T) {
	t.Parallel()
	s := OpenDefaultServer(NewConfig())
	defer s.Close()

	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: strings.Join([]string{
			fmt.Sprintf(`intmany,station=XiaoMaiDao01 value=2.0 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00Z").UnixNano()),
			fmt.Sprintf(`intmany,station=XiaoMaiDao02 value=4.0 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:10Z").UnixNano()),
			fmt.Sprintf(`intmany,station=XiaoMaiDao03 value=4.0 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:20Z").UnixNano()),
			fmt.Sprintf(`intmany,station=XiaoMaiDao04 value=4.0 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:30Z").UnixNano()),
			fmt.Sprintf(`intmany,station=XiaoMaiDao05 value=5.0 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:40Z").UnixNano()),
			fmt.Sprintf(`intmany,station=XiaoMaiDao06 value=5.0 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:50Z").UnixNano()),
			fmt.Sprintf(`intmany,station=XiaoMaiDao07 value=7.0 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:01:00Z").UnixNano()),
			fmt.Sprintf(`intmany,station=XiaoMaiDao08 value=9.0 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:01:10Z").UnixNano()),
		}, "\n")},
	}

	test.addQueries([]*Query{
		&Query{
			name:    "mean and stddev - int",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT MEAN(value), STDDEV(value) FROM intmany WHERE time >= '2000-01-01' AND time < '2000-01-01T00:02:00Z' GROUP BY time(10m)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"intmany","columns":["time","mean","stddev"],"values":[["2000-01-01T00:00:00Z",5,2.138089935299395]]}]}]}`,
		},
		&Query{
			name:    "first - int",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT FIRST(value) FROM intmany`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"intmany","columns":["time","first"],"values":[["2000-01-01T00:00:00Z",2]]}]}]}`,
		},
		&Query{
			name:    "first - int - epoch ms",
			params:  url.Values{"db": []string{"db0"}, "epoch": []string{"ms"}},
			command: `SELECT FIRST(value) FROM intmany`,
			exp:     fmt.Sprintf(`{"results":[{"statement_id":0,"series":[{"name":"intmany","columns":["time","first"],"values":[[%d,2]]}]}]}`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00Z").UnixNano()/int64(time.Millisecond)),
		},
		&Query{
			name:    "last - int",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT LAST(value) FROM intmany`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"intmany","columns":["time","last"],"values":[["2000-01-01T00:01:10Z",9]]}]}]}`,
		},
		&Query{
			name:    "spread - int",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT SPREAD(value) FROM intmany`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"intmany","columns":["time","spread"],"values":[["1970-01-01T00:00:00Z",7]]}]}]}`,
		},
		&Query{
			name:    "median - even count - int",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT MEDIAN(value) FROM intmany`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"intmany","columns":["time","median"],"values":[["1970-01-01T00:00:00Z",4.5]]}]}]}`,
		},
		&Query{
			name:    "median - odd count - int",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT MEDIAN(value) FROM intmany where time < '2000-01-01T00:01:10Z'`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"intmany","columns":["time","median"],"values":[["1970-01-01T00:00:00Z",4]]}]}]}`,
		},
		&Query{
			name:    "mode - single - int",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT MODE(value) FROM intmany`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"intmany","columns":["time","mode"],"values":[["1970-01-01T00:00:00Z",4]]}]}]}`,
		},
		&Query{
			name:    "mode - multiple - int",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT MODE(value) FROM intmany where time < '2000-01-01T00:01:10Z'`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"intmany","columns":["time","mode"],"values":[["1970-01-01T00:00:00Z",4]]}]}]}`,
		},
		&Query{
			name:    "distinct as call - int",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT DISTINCT(value) FROM intmany`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"intmany","columns":["time","distinct"],"values":[["1970-01-01T00:00:00Z",2],["1970-01-01T00:00:00Z",4],["1970-01-01T00:00:00Z",5],["1970-01-01T00:00:00Z",7],["1970-01-01T00:00:00Z",9]]}]}]}`,
		},
		&Query{
			name:    "distinct alt syntax - int",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT DISTINCT value FROM intmany`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"intmany","columns":["time","distinct"],"values":[["1970-01-01T00:00:00Z",2],["1970-01-01T00:00:00Z",4],["1970-01-01T00:00:00Z",5],["1970-01-01T00:00:00Z",7],["1970-01-01T00:00:00Z",9]]}]}]}`,
		},
		&Query{
			name:    "count distinct - int",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT COUNT(DISTINCT value) FROM intmany`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"intmany","columns":["time","count"],"values":[["1970-01-01T00:00:00Z",5]]}]}]}`,
		},
		&Query{
			name:    "count distinct as call - int",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT COUNT(DISTINCT(value)) FROM intmany`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"intmany","columns":["time","count"],"values":[["1970-01-01T00:00:00Z",5]]}]}]}`,
		},
	}...)

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

func TestServer_Query_Aggregates_IntMany_GroupBy(t *testing.T) {
	t.Parallel()
	s := OpenDefaultServer(NewConfig())
	defer s.Close()

	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: strings.Join([]string{
			fmt.Sprintf(`intmany,station=server01 value=2.0 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00Z").UnixNano()),
			fmt.Sprintf(`intmany,station=server02 value=4.0 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:10Z").UnixNano()),
			fmt.Sprintf(`intmany,station=server03 value=4.0 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:20Z").UnixNano()),
			fmt.Sprintf(`intmany,station=server04 value=4.0 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:30Z").UnixNano()),
			fmt.Sprintf(`intmany,station=server05 value=5.0 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:40Z").UnixNano()),
			fmt.Sprintf(`intmany,station=server06 value=5.0 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:50Z").UnixNano()),
			fmt.Sprintf(`intmany,station=server07 value=7.0 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:01:00Z").UnixNano()),
			fmt.Sprintf(`intmany,station=server08 value=9.0 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:01:10Z").UnixNano()),
		}, "\n")},
	}

	test.addQueries([]*Query{
		&Query{
			name:    "max order by time with time specified group by 10s",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT time, max(value) FROM intmany where time >= '2000-01-01T00:00:00Z' AND time <= '2000-01-01T00:01:14Z' group by time(10s)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"intmany","columns":["time","max"],"values":[["2000-01-01T00:00:00Z",2],["2000-01-01T00:00:10Z",4],["2000-01-01T00:00:20Z",4],["2000-01-01T00:00:30Z",4],["2000-01-01T00:00:40Z",5],["2000-01-01T00:00:50Z",5],["2000-01-01T00:01:00Z",7],["2000-01-01T00:01:10Z",9]]}]}]}`,
		},
		&Query{
			name:    "max order by time without time specified group by 30s",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT max(value) FROM intmany where time >= '2000-01-01T00:00:00Z' AND time <= '2000-01-01T00:01:14Z' group by time(30s)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"intmany","columns":["time","max"],"values":[["2000-01-01T00:00:00Z",4],["2000-01-01T00:00:30Z",5],["2000-01-01T00:01:00Z",9]]}]}]}`,
		},
		&Query{
			name:    "max order by time with time specified group by 30s",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT time, max(value) FROM intmany where time >= '2000-01-01T00:00:00Z' AND time <= '2000-01-01T00:01:14Z' group by time(30s)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"intmany","columns":["time","max"],"values":[["2000-01-01T00:00:00Z",4],["2000-01-01T00:00:30Z",5],["2000-01-01T00:01:00Z",9]]}]}]}`,
		},
		&Query{
			name:    "min order by time without time specified group by 15s",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT min(value) FROM intmany where time >= '2000-01-01T00:00:00Z' AND time <= '2000-01-01T00:01:14Z' group by time(15s)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"intmany","columns":["time","min"],"values":[["2000-01-01T00:00:00Z",2],["2000-01-01T00:00:15Z",4],["2000-01-01T00:00:30Z",4],["2000-01-01T00:00:45Z",5],["2000-01-01T00:01:00Z",7]]}]}]}`,
		},
		&Query{
			name:    "min order by time with time specified group by 15s",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT time, min(value) FROM intmany where time >= '2000-01-01T00:00:00Z' AND time <= '2000-01-01T00:01:14Z' group by time(15s)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"intmany","columns":["time","min"],"values":[["2000-01-01T00:00:00Z",2],["2000-01-01T00:00:15Z",4],["2000-01-01T00:00:30Z",4],["2000-01-01T00:00:45Z",5],["2000-01-01T00:01:00Z",7]]}]}]}`,
		},
		&Query{
			name:    "first order by time without time specified group by 15s",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT first(value) FROM intmany where time >= '2000-01-01T00:00:00Z' AND time <= '2000-01-01T00:01:14Z' group by time(15s)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"intmany","columns":["time","first"],"values":[["2000-01-01T00:00:00Z",2],["2000-01-01T00:00:15Z",4],["2000-01-01T00:00:30Z",4],["2000-01-01T00:00:45Z",5],["2000-01-01T00:01:00Z",7]]}]}]}`,
		},
		&Query{
			name:    "first order by time with time specified group by 15s",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT time, first(value) FROM intmany where time >= '2000-01-01T00:00:00Z' AND time <= '2000-01-01T00:01:14Z' group by time(15s)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"intmany","columns":["time","first"],"values":[["2000-01-01T00:00:00Z",2],["2000-01-01T00:00:15Z",4],["2000-01-01T00:00:30Z",4],["2000-01-01T00:00:45Z",5],["2000-01-01T00:01:00Z",7]]}]}]}`,
		},
		&Query{
			name:    "last order by time without time specified group by 15s",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT last(value) FROM intmany where time >= '2000-01-01T00:00:00Z' AND time <= '2000-01-01T00:01:14Z' group by time(15s)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"intmany","columns":["time","last"],"values":[["2000-01-01T00:00:00Z",4],["2000-01-01T00:00:15Z",4],["2000-01-01T00:00:30Z",5],["2000-01-01T00:00:45Z",5],["2000-01-01T00:01:00Z",9]]}]}]}`,
		},
		&Query{
			name:    "last order by time with time specified group by 15s",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT time, last(value) FROM intmany where time >= '2000-01-01T00:00:00Z' AND time <= '2000-01-01T00:01:14Z' group by time(15s)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"intmany","columns":["time","last"],"values":[["2000-01-01T00:00:00Z",4],["2000-01-01T00:00:15Z",4],["2000-01-01T00:00:30Z",5],["2000-01-01T00:00:45Z",5],["2000-01-01T00:01:00Z",9]]}]}]}`,
		},
	}...)

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

func TestServer_Query_Aggregates_IntMany_OrderByDesc(t *testing.T) {
	t.Parallel()
	s := OpenDefaultServer(NewConfig())
	defer s.Close()

	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: strings.Join([]string{
			fmt.Sprintf(`intmany,station=server01 value=2.0 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00Z").UnixNano()),
			fmt.Sprintf(`intmany,station=server02 value=4.0 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:10Z").UnixNano()),
			fmt.Sprintf(`intmany,station=server03 value=4.0 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:20Z").UnixNano()),
			fmt.Sprintf(`intmany,station=server04 value=4.0 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:30Z").UnixNano()),
			fmt.Sprintf(`intmany,station=server05 value=5.0 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:40Z").UnixNano()),
			fmt.Sprintf(`intmany,station=server06 value=5.0 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:50Z").UnixNano()),
			fmt.Sprintf(`intmany,station=server07 value=7.0 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:01:00Z").UnixNano()),
			fmt.Sprintf(`intmany,station=server08 value=9.0 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:01:10Z").UnixNano()),
		}, "\n")},
	}

	test.addQueries([]*Query{
		&Query{
			name:    "aggregate order by time desc",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT max(value) FROM intmany where time >= '2000-01-01T00:00:00Z' AND time <= '2000-01-01T00:01:00Z' group by time(10s) order by time desc`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"intmany","columns":["time","max"],"values":[["2000-01-01T00:01:00Z",7],["2000-01-01T00:00:50Z",5],["2000-01-01T00:00:40Z",5],["2000-01-01T00:00:30Z",4],["2000-01-01T00:00:20Z",4],["2000-01-01T00:00:10Z",4],["2000-01-01T00:00:00Z",2]]}]}]}`,
		},
	}...)

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

func TestServer_Query_Aggregates_IntOverlap(t *testing.T) {
	t.Parallel()
	s := OpenDefaultServer(NewConfig())
	defer s.Close()

	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: strings.Join([]string{
			fmt.Sprintf(`intoverlap,region=us-east value=20 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00Z").UnixNano()),
			fmt.Sprintf(`intoverlap,region=us-east value=30 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:10Z").UnixNano()),
			fmt.Sprintf(`intoverlap,region=us-west value=100 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00Z").UnixNano()),
			fmt.Sprintf(`intoverlap,region=us-east otherVal=20 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:03Z").UnixNano()),
		}, "\n")},
	}

	test.addQueries([]*Query{
		&Query{
			name:    "aggregation with a null field value - int",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT SUM(value) FROM intoverlap GROUP BY region`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"intoverlap","tags":{"region":"us-east"},"columns":["time","sum"],"values":[["1970-01-01T00:00:00Z",50]]},{"name":"intoverlap","tags":{"region":"us-west"},"columns":["time","sum"],"values":[["1970-01-01T00:00:00Z",100]]}]}]}`,
		},
		&Query{
			name:    "multiple aggregations - int",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT SUM(value), MEAN(value) FROM intoverlap GROUP BY region`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"intoverlap","tags":{"region":"us-east"},"columns":["time","sum","mean"],"values":[["1970-01-01T00:00:00Z",50,25]]},{"name":"intoverlap","tags":{"region":"us-west"},"columns":["time","sum","mean"],"values":[["1970-01-01T00:00:00Z",100,100]]}]}]}`,
		},
	}...)

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

func TestServer_Query_Aggregates_FloatSingle(t *testing.T) {
	t.Parallel()
	s := OpenDefaultServer(NewConfig())
	defer s.Close()

	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: strings.Join([]string{
			fmt.Sprintf(`floatsingle value=45.0 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00Z").UnixNano()),
		}, "\n")},
	}

	test.addQueries([]*Query{
		&Query{
			name:    "stddev with just one point - float",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT STDDEV(value) FROM floatsingle`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"floatsingle","columns":["time","stddev"],"values":[["1970-01-01T00:00:00Z",null]]}]}]}`,
		},
	}...)

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

func TestServer_Query_Aggregates_FloatMany(t *testing.T) {
	t.Parallel()
	s := OpenDefaultServer(NewConfig())
	defer s.Close()

	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: strings.Join([]string{
			fmt.Sprintf(`floatmany,station=server01 value=2.0 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00Z").UnixNano()),
			fmt.Sprintf(`floatmany,station=server02 value=4.0 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:10Z").UnixNano()),
			fmt.Sprintf(`floatmany,station=server03 value=4.0 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:20Z").UnixNano()),
			fmt.Sprintf(`floatmany,station=server04 value=4.0 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:30Z").UnixNano()),
			fmt.Sprintf(`floatmany,station=server05 value=5.0 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:40Z").UnixNano()),
			fmt.Sprintf(`floatmany,station=server06 value=5.0 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:50Z").UnixNano()),
			fmt.Sprintf(`floatmany,station=server07 value=7.0 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:01:00Z").UnixNano()),
			fmt.Sprintf(`floatmany,station=server08 value=9.0 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:01:10Z").UnixNano()),
		}, "\n")},
	}

	test.addQueries([]*Query{
		&Query{
			name:    "mean and stddev - float",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT MEAN(value), STDDEV(value) FROM floatmany WHERE time >= '2000-01-01' AND time < '2000-01-01T00:02:00Z' GROUP BY time(10m)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"floatmany","columns":["time","mean","stddev"],"values":[["2000-01-01T00:00:00Z",5,2.138089935299395]]}]}]}`,
		},
		&Query{
			name:    "first - float",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT FIRST(value) FROM floatmany`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"floatmany","columns":["time","first"],"values":[["2000-01-01T00:00:00Z",2]]}]}]}`,
		},
		&Query{
			name:    "last - float",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT LAST(value) FROM floatmany`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"floatmany","columns":["time","last"],"values":[["2000-01-01T00:01:10Z",9]]}]}]}`,
		},
		&Query{
			name:    "spread - float",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT SPREAD(value) FROM floatmany`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"floatmany","columns":["time","spread"],"values":[["1970-01-01T00:00:00Z",7]]}]}]}`,
		},
		&Query{
			name:    "median - even count - float",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT MEDIAN(value) FROM floatmany`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"floatmany","columns":["time","median"],"values":[["1970-01-01T00:00:00Z",4.5]]}]}]}`,
		},
		&Query{
			name:    "median - odd count - float",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT MEDIAN(value) FROM floatmany where time < '2000-01-01T00:01:10Z'`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"floatmany","columns":["time","median"],"values":[["1970-01-01T00:00:00Z",4]]}]}]}`,
		},
		&Query{
			name:    "mode - single - float",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT MODE(value) FROM floatmany`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"floatmany","columns":["time","mode"],"values":[["1970-01-01T00:00:00Z",4]]}]}]}`,
		},
		&Query{
			name:    "mode - multiple - float",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT MODE(value) FROM floatmany where time < '2000-01-01T00:00:10Z'`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"floatmany","columns":["time","mode"],"values":[["1970-01-01T00:00:00Z",2]]}]}]}`,
		},
		&Query{
			name:    "distinct as call - float",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT DISTINCT(value) FROM floatmany`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"floatmany","columns":["time","distinct"],"values":[["1970-01-01T00:00:00Z",2],["1970-01-01T00:00:00Z",4],["1970-01-01T00:00:00Z",5],["1970-01-01T00:00:00Z",7],["1970-01-01T00:00:00Z",9]]}]}]}`,
		},
		&Query{
			name:    "distinct alt syntax - float",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT DISTINCT value FROM floatmany`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"floatmany","columns":["time","distinct"],"values":[["1970-01-01T00:00:00Z",2],["1970-01-01T00:00:00Z",4],["1970-01-01T00:00:00Z",5],["1970-01-01T00:00:00Z",7],["1970-01-01T00:00:00Z",9]]}]}]}`,
		},
		&Query{
			name:    "count distinct - float",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT COUNT(DISTINCT value) FROM floatmany`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"floatmany","columns":["time","count"],"values":[["1970-01-01T00:00:00Z",5]]}]}]}`,
		},
		&Query{
			name:    "count distinct as call - float",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT COUNT(DISTINCT(value)) FROM floatmany`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"floatmany","columns":["time","count"],"values":[["1970-01-01T00:00:00Z",5]]}]}]}`,
		},
	}...)

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

func TestServer_Query_Aggregates_FloatOverlap(t *testing.T) {
	t.Parallel()
	s := OpenDefaultServer(NewConfig())
	defer s.Close()

	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: strings.Join([]string{
			fmt.Sprintf(`floatoverlap,region=us-east value=20.0 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00Z").UnixNano()),
			fmt.Sprintf(`floatoverlap,region=us-east value=30.0 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:10Z").UnixNano()),
			fmt.Sprintf(`floatoverlap,region=us-west value=100.0 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00Z").UnixNano()),
			fmt.Sprintf(`floatoverlap,region=us-east otherVal=20.0 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:03Z").UnixNano()),
		}, "\n")},
	}

	test.addQueries([]*Query{
		&Query{
			name:    "aggregation with no interval - float",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT count(value) FROM floatoverlap WHERE time = '2000-01-01 00:00:00'`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"floatoverlap","columns":["time","count"],"values":[["2000-01-01T00:00:00Z",2]]}]}]}`,
		},
		&Query{
			name:    "sum - float",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT SUM(value) FROM floatoverlap WHERE time >= '2000-01-01 00:00:05' AND time <= '2000-01-01T00:00:10Z' GROUP BY time(10s), region`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"floatoverlap","tags":{"region":"us-east"},"columns":["time","sum"],"values":[["2000-01-01T00:00:00Z",null],["2000-01-01T00:00:10Z",30]]}]}]}`,
		},
		&Query{
			name:    "aggregation with a null field value - float",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT SUM(value) FROM floatoverlap GROUP BY region`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"floatoverlap","tags":{"region":"us-east"},"columns":["time","sum"],"values":[["1970-01-01T00:00:00Z",50]]},{"name":"floatoverlap","tags":{"region":"us-west"},"columns":["time","sum"],"values":[["1970-01-01T00:00:00Z",100]]}]}]}`,
		},
		&Query{
			name:    "multiple aggregations - float",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT SUM(value), MEAN(value) FROM floatoverlap GROUP BY region`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"floatoverlap","tags":{"region":"us-east"},"columns":["time","sum","mean"],"values":[["1970-01-01T00:00:00Z",50,25]]},{"name":"floatoverlap","tags":{"region":"us-west"},"columns":["time","sum","mean"],"values":[["1970-01-01T00:00:00Z",100,100]]}]}]}`,
		},
		&Query{
			name:    "multiple aggregations with division - float",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT sum(value) / mean(value) as div FROM floatoverlap GROUP BY region`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"floatoverlap","tags":{"region":"us-east"},"columns":["time","div"],"values":[["1970-01-01T00:00:00Z",2]]},{"name":"floatoverlap","tags":{"region":"us-west"},"columns":["time","div"],"values":[["1970-01-01T00:00:00Z",1]]}]}]}`,
		},
	}...)

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

func TestServer_Query_Aggregates_GroupByOffset(t *testing.T) {
	t.Parallel()
	s := OpenDefaultServer(NewConfig())
	defer s.Close()

	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: strings.Join([]string{
			fmt.Sprintf(`offset,region=us-east,station=serverA value=20.0 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00Z").UnixNano()),
			fmt.Sprintf(`offset,region=us-east,station=serverB value=30.0 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:10Z").UnixNano()),
			fmt.Sprintf(`offset,region=us-west,station=serverC value=100.0 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00Z").UnixNano()),
		}, "\n")},
	}

	test.addQueries([]*Query{
		&Query{
			name:    "group by offset - standard",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT sum(value) FROM "offset" WHERE time >= '1999-12-31T23:59:55Z' AND time < '2000-01-01T00:00:15Z' GROUP BY time(10s, 5s) FILL(0)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"offset","columns":["time","sum"],"values":[["1999-12-31T23:59:55Z",120],["2000-01-01T00:00:05Z",30]]}]}]}`,
		},
		&Query{
			name:    "group by offset - misaligned time",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT sum(value) FROM "offset" WHERE time >= '2000-01-01T00:00:00Z' AND time < '2000-01-01T00:00:20Z' GROUP BY time(10s, 5s) FILL(0)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"offset","columns":["time","sum"],"values":[["1999-12-31T23:59:55Z",120],["2000-01-01T00:00:05Z",30],["2000-01-01T00:00:15Z",0]]}]}]}`,
		},
		&Query{
			name:    "group by offset - negative time",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT sum(value) FROM "offset" WHERE time >= '1999-12-31T23:59:55Z' AND time < '2000-01-01T00:00:15Z' GROUP BY time(10s, -5s) FILL(0)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"offset","columns":["time","sum"],"values":[["1999-12-31T23:59:55Z",120],["2000-01-01T00:00:05Z",30]]}]}]}`,
		},
		&Query{
			name:    "group by offset - modulo",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT sum(value) FROM "offset" WHERE time >= '1999-12-31T23:59:55Z' AND time < '2000-01-01T00:00:15Z' GROUP BY time(10s, 35s) FILL(0)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"offset","columns":["time","sum"],"values":[["1999-12-31T23:59:55Z",120],["2000-01-01T00:00:05Z",30]]}]}]}`,
		},
	}...)

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

func TestServer_Query_Aggregates_Load(t *testing.T) {
	t.Parallel()
	s := OpenDefaultServer(NewConfig())
	defer s.Close()

	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: strings.Join([]string{
			fmt.Sprintf(`load,region=us-east,station=serverA value=20.0 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00Z").UnixNano()),
			fmt.Sprintf(`load,region=us-east,station=serverB value=30.0 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:10Z").UnixNano()),
			fmt.Sprintf(`load,region=us-west,station=serverC value=100.0 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00Z").UnixNano()),
		}, "\n")},
	}

	test.addQueries([]*Query{
		&Query{
			name:    "group by multiple dimensions",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT sum(value) FROM load GROUP BY region, station`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"load","tags":{"region":"us-east","station":"serverA"},"columns":["time","sum"],"values":[["1970-01-01T00:00:00Z",20]]},{"name":"load","tags":{"region":"us-east","station":"serverB"},"columns":["time","sum"],"values":[["1970-01-01T00:00:00Z",30]]},{"name":"load","tags":{"region":"us-west","station":"serverC"},"columns":["time","sum"],"values":[["1970-01-01T00:00:00Z",100]]}]}]}`,
		},
		&Query{
			name:    "group by multiple dimensions",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT sum(value)*2 FROM load`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"load","columns":["time","sum"],"values":[["1970-01-01T00:00:00Z",300]]}]}]}`,
		},
		&Query{
			name:    "group by multiple dimensions",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT sum(value)/2 FROM load`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"load","columns":["time","sum"],"values":[["1970-01-01T00:00:00Z",75]]}]}]}`,
		},
	}...)

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

func TestServer_Query_Aggregates_AIR(t *testing.T) {
	t.Parallel()
	s := OpenDefaultServer(NewConfig())
	defer s.Close()

	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: strings.Join([]string{
			fmt.Sprintf(`air,region=uk,station=XiaoMaiDaoZ,weather=cloud value=20.0 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:03Z").UnixNano()),
			fmt.Sprintf(`air,region=uk,station=XiaoMaiDaoZ,weather=rain value=30.0 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:03Z").UnixNano()),
		}, "\n")},
	}

	test.addQueries([]*Query{
		&Query{
			name:    "aggregation with WHERE and AND",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT sum(value) FROM air WHERE region='uk' AND station='XiaoMaiDaoZ'`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","sum"],"values":[["1970-01-01T00:00:00Z",50]]}]}]}`,
		},
	}...)

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

func TestServer_Query_Aggregates_Math(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig())
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("rp0", 1, 0), true); err != nil {
		t.Fatal(err)
	}

	writes := []string{
		fmt.Sprintf(`air,station=XiaoMaiDao01,region=west,Moisture=1 visibility=10i,pressure=20i,Moisture=2i %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00Z").UnixNano()),
		fmt.Sprintf(`air,station=XiaoMaiDao02,region=west,Moisture=2 visibility=40i,pressure=50i,Moisture=3i %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:10Z").UnixNano()),
		fmt.Sprintf(`air,station=XiaoMaiDao03,region=east,Moisture=3 visibility=40i,pressure=55i,Moisture=4i %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:20Z").UnixNano()),
		fmt.Sprintf(`air,station=XiaoMaiDao04,region=east,Moisture=4 visibility=40i,pressure=60i,Moisture=1i %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:30Z").UnixNano()),
		fmt.Sprintf(`air,station=XiaoMaiDao05,region=west,Moisture=1 visibility=50i,pressure=70i,Moisture=2i %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:40Z").UnixNano()),
		fmt.Sprintf(`air,station=XiaoMaiDao06,region=east,Moisture=2 visibility=50i,pressure=40i,Moisture=3i %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:50Z").UnixNano()),
		fmt.Sprintf(`air,station=XiaoMaiDao07,region=west,Moisture=3 visibility=70i,pressure=30i,Moisture=4i %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:01:00Z").UnixNano()),
		fmt.Sprintf(`air,station=XiaoMaiDao08,region=east,Moisture=4 visibility=90i,pressure=10i,Moisture=1i %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:01:10Z").UnixNano()),
		fmt.Sprintf(`air,station=XiaoMaiDao09,region=east,Moisture=1 visibility=5i,pressure=4i,Moisture=2i %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:01:20Z").UnixNano()),
	}

	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}

	test.addQueries([]*Query{
		&Query{
			name:    "add two selectors",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT max(visibility) + min(visibility) FROM air WHERE time >= '2000-01-01T00:00:00Z' AND time < '2000-01-01T00:01:30Z'`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","max_min"],"values":[["2000-01-01T00:00:00Z",95]]}]}]}`,
		},
		&Query{
			name:    "use math one two selectors separately",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT max(visibility) * 1, min(visibility) * 1 FROM air WHERE time >= '2000-01-01T00:00:00Z' AND time < '2000-01-01T00:01:30Z'`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","max","min"],"values":[["2000-01-01T00:00:00Z",90,5]]}]}]}`,
		},
		&Query{
			name:    "math with a single selector",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT max(visibility) * 1 FROM air WHERE time >= '2000-01-01T00:00:00Z' AND time < '2000-01-01T00:01:30Z'`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","max"],"values":[["2000-01-01T00:01:10Z",90]]}]}]}`,
		},
	}...)

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

func TestServer_Query_AggregateSelectors(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig())
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("rp0", 1, 0), true); err != nil {
		t.Fatal(err)
	}

	writes := []string{
		fmt.Sprintf(`air,station=XiaoMaiDao01,region=west,Moisture=1 visibility=10i,pressure=20i,Moisture=2i %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00Z").UnixNano()),
		fmt.Sprintf(`air,station=XiaoMaiDao02,region=west,Moisture=2 visibility=40i,pressure=50i,Moisture=3i %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:10Z").UnixNano()),
		fmt.Sprintf(`air,station=XiaoMaiDao03,region=east,Moisture=3 visibility=40i,pressure=55i,Moisture=4i %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:20Z").UnixNano()),
		fmt.Sprintf(`air,station=XiaoMaiDao04,region=east,Moisture=4 visibility=40i,pressure=60i,Moisture=1i %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:30Z").UnixNano()),
		fmt.Sprintf(`air,station=XiaoMaiDao05,region=west,Moisture=1 visibility=50i,pressure=70i,Moisture=2i %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:40Z").UnixNano()),
		fmt.Sprintf(`air,station=XiaoMaiDao06,region=east,Moisture=2 visibility=50i,pressure=40i,Moisture=3i %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:50Z").UnixNano()),
		fmt.Sprintf(`air,station=XiaoMaiDao07,region=west,Moisture=3 visibility=70i,pressure=30i,Moisture=4i %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:01:00Z").UnixNano()),
		fmt.Sprintf(`air,station=XiaoMaiDao08,region=east,Moisture=4 visibility=90i,pressure=10i,Moisture=1i %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:01:10Z").UnixNano()),
		fmt.Sprintf(`air,station=XiaoMaiDao09,region=east,Moisture=1 visibility=5i,pressure=4i,Moisture=2i %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:01:20Z").UnixNano()),
	}

	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}

	test.addQueries([]*Query{
		&Query{
			name:    "baseline",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT * FROM air`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","Moisture","Moisture_1","pressure","region","station","visibility"],"values":[["2000-01-01T00:00:00Z",2,"1",20,"west","XiaoMaiDao01",10],["2000-01-01T00:00:10Z",3,"2",50,"west","XiaoMaiDao02",40],["2000-01-01T00:00:20Z",4,"3",55,"east","XiaoMaiDao03",40],["2000-01-01T00:00:30Z",1,"4",60,"east","XiaoMaiDao04",40],["2000-01-01T00:00:40Z",2,"1",70,"west","XiaoMaiDao05",50],["2000-01-01T00:00:50Z",3,"2",40,"east","XiaoMaiDao06",50],["2000-01-01T00:01:00Z",4,"3",30,"west","XiaoMaiDao07",70],["2000-01-01T00:01:10Z",1,"4",10,"east","XiaoMaiDao08",90],["2000-01-01T00:01:20Z",2,"1",4,"east","XiaoMaiDao09",5]]}]}]}`,
        },
		&Query{
			name:    "max - baseline 30s",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT max(visibility) FROM air where time >= '2000-01-01T00:00:00Z' AND time <= '2000-01-01T00:01:29Z' group by time(30s)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","max"],"values":[["2000-01-01T00:00:00Z",40],["2000-01-01T00:00:30Z",50],["2000-01-01T00:01:00Z",90]]}]}]}`,
		},
		&Query{
			name:    "max - baseline 30s - epoch ms",
			params:  url.Values{"db": []string{"db0"}, "epoch": []string{"ms"}},
			command: `SELECT max(visibility) FROM air where time >= '2000-01-01T00:00:00Z' AND time <= '2000-01-01T00:01:29Z' group by time(30s)`,
			exp: fmt.Sprintf(
				`{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","max"],"values":[[%d,40],[%d,50],[%d,90]]}]}]}`,
				mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00Z").UnixNano()/int64(time.Millisecond),
				mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:30Z").UnixNano()/int64(time.Millisecond),
				mustParseTime(time.RFC3339Nano, "2000-01-01T00:01:00Z").UnixNano()/int64(time.Millisecond),
			),
		},
		&Query{
			name:    "max - pressure",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT pressure, max(visibility) FROM air where time >= '2000-01-01T00:00:00Z' AND time <= '2000-01-01T00:01:29Z' group by time(30s)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","pressure","max"],"values":[["2000-01-01T00:00:00Z",50,40],["2000-01-01T00:00:30Z",70,50],["2000-01-01T00:01:00Z",10,90]]}]}]}`,
		},
		&Query{
			name:    "max - time",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT time, max(visibility) FROM air where time >= '2000-01-01T00:00:00Z' AND time <= '2000-01-01T00:01:29Z' group by time(30s)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","max"],"values":[["2000-01-01T00:00:00Z",40],["2000-01-01T00:00:30Z",50],["2000-01-01T00:01:00Z",90]]}]}]}`,
		},
		&Query{
			name:    "max - time and pressure",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT time, pressure, max(visibility) FROM air where time >= '2000-01-01T00:00:00Z' AND time <= '2000-01-01T00:01:29Z' group by time(30s)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","pressure","max"],"values":[["2000-01-01T00:00:00Z",50,40],["2000-01-01T00:00:30Z",70,50],["2000-01-01T00:01:00Z",10,90]]}]}]}`,
		},
		&Query{
			name:    "min - baseline 30s",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT min(visibility) FROM air where time >= '2000-01-01T00:00:00Z' AND time <= '2000-01-01T00:01:29Z' group by time(30s)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","min"],"values":[["2000-01-01T00:00:00Z",10],["2000-01-01T00:00:30Z",40],["2000-01-01T00:01:00Z",5]]}]}]}`,
		},
		&Query{
			name:    "min - pressure",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT pressure, min(visibility) FROM air where time >= '2000-01-01T00:00:00Z' AND time <= '2000-01-01T00:01:29Z' group by time(30s)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","pressure","min"],"values":[["2000-01-01T00:00:00Z",20,10],["2000-01-01T00:00:30Z",60,40],["2000-01-01T00:01:00Z",4,5]]}]}]}`,
		},
		&Query{
			name:    "min - time",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT time, min(visibility) FROM air where time >= '2000-01-01T00:00:00Z' AND time <= '2000-01-01T00:01:29Z' group by time(30s)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","min"],"values":[["2000-01-01T00:00:00Z",10],["2000-01-01T00:00:30Z",40],["2000-01-01T00:01:00Z",5]]}]}]}`,
		},
		&Query{
			name:    "min - time and pressure",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT time, pressure, min(visibility) FROM air where time >= '2000-01-01T00:00:00Z' AND time <= '2000-01-01T00:01:29Z' group by time(30s)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","pressure","min"],"values":[["2000-01-01T00:00:00Z",20,10],["2000-01-01T00:00:30Z",60,40],["2000-01-01T00:01:00Z",4,5]]}]}]}`,
		},
		&Query{
			name:    "max,min - baseline 30s",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT max(visibility), min(visibility) FROM air where time >= '2000-01-01T00:00:00Z' AND time <= '2000-01-01T00:01:29Z' group by time(30s)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","max","min"],"values":[["2000-01-01T00:00:00Z",40,10],["2000-01-01T00:00:30Z",50,40],["2000-01-01T00:01:00Z",90,5]]}]}]}`,
		},
		&Query{
			name:    "first - baseline 30s",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT first(visibility) FROM air where time >= '2000-01-01T00:00:00Z' AND time <= '2000-01-01T00:01:29Z' group by time(30s)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","first"],"values":[["2000-01-01T00:00:00Z",10],["2000-01-01T00:00:30Z",40],["2000-01-01T00:01:00Z",70]]}]}]}`,
		},
		&Query{
			name:    "first - pressure",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT time, pressure, first(visibility) FROM air where time >= '2000-01-01T00:00:00Z' AND time <= '2000-01-01T00:01:29Z' group by time(30s)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","pressure","first"],"values":[["2000-01-01T00:00:00Z",20,10],["2000-01-01T00:00:30Z",60,40],["2000-01-01T00:01:00Z",30,70]]}]}]}`,
		},
		&Query{
			name:    "first - time",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT time, first(visibility) FROM air where time >= '2000-01-01T00:00:00Z' AND time <= '2000-01-01T00:01:29Z' group by time(30s)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","first"],"values":[["2000-01-01T00:00:00Z",10],["2000-01-01T00:00:30Z",40],["2000-01-01T00:01:00Z",70]]}]}]}`,
		},
		&Query{
			name:    "first - time and pressure",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT time, pressure, first(visibility) FROM air where time >= '2000-01-01T00:00:00Z' AND time <= '2000-01-01T00:01:29Z' group by time(30s)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","pressure","first"],"values":[["2000-01-01T00:00:00Z",20,10],["2000-01-01T00:00:30Z",60,40],["2000-01-01T00:01:00Z",30,70]]}]}]}`,
		},
		&Query{
			name:    "last - baseline 30s",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT last(visibility) FROM air where time >= '2000-01-01T00:00:00Z' AND time <= '2000-01-01T00:01:29Z' group by time(30s)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","last"],"values":[["2000-01-01T00:00:00Z",40],["2000-01-01T00:00:30Z",50],["2000-01-01T00:01:00Z",5]]}]}]}`,
		},
		&Query{
			name:    "last - pressure",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT pressure, last(visibility) FROM air where time >= '2000-01-01T00:00:00Z' AND time <= '2000-01-01T00:01:29Z' group by time(30s)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","pressure","last"],"values":[["2000-01-01T00:00:00Z",55,40],["2000-01-01T00:00:30Z",40,50],["2000-01-01T00:01:00Z",4,5]]}]}]}`,
		},
		&Query{
			name:    "last - time",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT time, last(visibility) FROM air where time >= '2000-01-01T00:00:00Z' AND time <= '2000-01-01T00:01:29Z' group by time(30s)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","last"],"values":[["2000-01-01T00:00:00Z",40],["2000-01-01T00:00:30Z",50],["2000-01-01T00:01:00Z",5]]}]}]}`,
		},
		&Query{
			name:    "last - time and pressure",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT time, pressure, last(visibility) FROM air where time >= '2000-01-01T00:00:00Z' AND time <= '2000-01-01T00:01:29Z' group by time(30s)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","pressure","last"],"values":[["2000-01-01T00:00:00Z",55,40],["2000-01-01T00:00:30Z",40,50],["2000-01-01T00:01:00Z",4,5]]}]}]}`,
		},
		&Query{
			name:    "count - baseline 30s",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT count(visibility) FROM air where time >= '2000-01-01T00:00:00Z' AND time <= '2000-01-01T00:01:29Z' group by time(30s)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","count"],"values":[["2000-01-01T00:00:00Z",3],["2000-01-01T00:00:30Z",3],["2000-01-01T00:01:00Z",3]]}]}]}`,
		},
		&Query{
			name:    "count - time",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT time, count(visibility) FROM air where time >= '2000-01-01T00:00:00Z' AND time <= '2000-01-01T00:01:29Z' group by time(30s)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","count"],"values":[["2000-01-01T00:00:00Z",3],["2000-01-01T00:00:30Z",3],["2000-01-01T00:01:00Z",3]]}]}]}`,
		},
		&Query{
			name:    "count - pressure",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT pressure, count(visibility) FROM air where time >= '2000-01-01T00:00:00Z' AND time <= '2000-01-01T00:01:29Z' group by time(30s)`,
			exp:     `{"results":[{"statement_id":0,"error":"mixing aggregate and non-aggregate queries is not supported"}]}`,
		},
		&Query{
			name:    "distinct - baseline 30s",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT distinct(visibility) FROM air where time >= '2000-01-01T00:00:00Z' AND time <= '2000-01-01T00:01:29Z' group by time(30s)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","distinct"],"values":[["2000-01-01T00:00:00Z",10],["2000-01-01T00:00:00Z",40],["2000-01-01T00:00:30Z",40],["2000-01-01T00:00:30Z",50],["2000-01-01T00:01:00Z",70],["2000-01-01T00:01:00Z",90],["2000-01-01T00:01:00Z",5]]}]}]}`,
		},
		&Query{
			name:    "distinct - time",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT time, distinct(visibility) FROM air where time >= '2000-01-01T00:00:00Z' AND time <= '2000-01-01T00:01:29Z' group by time(30s)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","distinct"],"values":[["2000-01-01T00:00:00Z",10],["2000-01-01T00:00:00Z",40],["2000-01-01T00:00:30Z",40],["2000-01-01T00:00:30Z",50],["2000-01-01T00:01:00Z",70],["2000-01-01T00:01:00Z",90],["2000-01-01T00:01:00Z",5]]}]}]}`,
		},
		&Query{
			name:    "distinct - pressure",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT pressure, distinct(visibility) FROM air where time >= '2000-01-01T00:00:00Z' AND time <= '2000-01-01T00:01:29Z' group by time(30s)`,
			exp:     `{"results":[{"statement_id":0,"error":"aggregate function distinct() cannot be combined with other functions or fields"}]}`,
		},
		&Query{
			name:    "mean - baseline 30s",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT mean(visibility) FROM air where time >= '2000-01-01T00:00:00Z' AND time <= '2000-01-01T00:01:29Z' group by time(30s)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","mean"],"values":[["2000-01-01T00:00:00Z",30],["2000-01-01T00:00:30Z",46.666666666666664],["2000-01-01T00:01:00Z",55]]}]}]}`,
		},
		&Query{
			name:    "mean - time",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT time, mean(visibility) FROM air where time >= '2000-01-01T00:00:00Z' AND time <= '2000-01-01T00:01:29Z' group by time(30s)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","mean"],"values":[["2000-01-01T00:00:00Z",30],["2000-01-01T00:00:30Z",46.666666666666664],["2000-01-01T00:01:00Z",55]]}]}]}`,
		},
		&Query{
			name:    "mean - pressure",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT pressure, mean(visibility) FROM air where time >= '2000-01-01T00:00:00Z' AND time <= '2000-01-01T00:01:29Z' group by time(30s)`,
			exp:     `{"results":[{"statement_id":0,"error":"mixing aggregate and non-aggregate queries is not supported"}]}`,
		},
		&Query{
			name:    "median - baseline 30s",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT median(visibility) FROM air where time >= '2000-01-01T00:00:00Z' AND time <= '2000-01-01T00:01:29Z' group by time(30s)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","median"],"values":[["2000-01-01T00:00:00Z",40],["2000-01-01T00:00:30Z",50],["2000-01-01T00:01:00Z",70]]}]}]}`,
		},
		&Query{
			name:    "median - time",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT time, median(visibility) FROM air where time >= '2000-01-01T00:00:00Z' AND time <= '2000-01-01T00:01:29Z' group by time(30s)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","median"],"values":[["2000-01-01T00:00:00Z",40],["2000-01-01T00:00:30Z",50],["2000-01-01T00:01:00Z",70]]}]}]}`,
		},
		&Query{
			name:    "median - pressure",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT pressure, median(visibility) FROM air where time >= '2000-01-01T00:00:00Z' AND time <= '2000-01-01T00:01:29Z' group by time(30s)`,
			exp:     `{"results":[{"statement_id":0,"error":"mixing aggregate and non-aggregate queries is not supported"}]}`,
		},
		&Query{
			name:    "mode - baseline 30s",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT mode(visibility) FROM air where time >= '2000-01-01T00:00:00Z' AND time <= '2000-01-01T00:01:29Z' group by time(30s)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","mode"],"values":[["2000-01-01T00:00:00Z",40],["2000-01-01T00:00:30Z",50],["2000-01-01T00:01:00Z",5]]}]}]}`,
		},
		&Query{
			name:    "mode - time",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT time, mode(visibility) FROM air where time >= '2000-01-01T00:00:00Z' AND time <= '2000-01-01T00:01:29Z' group by time(30s)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","mode"],"values":[["2000-01-01T00:00:00Z",40],["2000-01-01T00:00:30Z",50],["2000-01-01T00:01:00Z",5]]}]}]}`,
		},
		&Query{
			name:    "mode - pressure",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT pressure, mode(visibility) FROM air where time >= '2000-01-01T00:00:00Z' AND time <= '2000-01-01T00:01:29Z' group by time(30s)`,
			exp:     `{"results":[{"statement_id":0,"error":"mixing aggregate and non-aggregate queries is not supported"}]}`,
		},
		&Query{
			name:    "spread - baseline 30s",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT spread(visibility) FROM air where time >= '2000-01-01T00:00:00Z' AND time <= '2000-01-01T00:01:29Z' group by time(30s)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","spread"],"values":[["2000-01-01T00:00:00Z",30],["2000-01-01T00:00:30Z",10],["2000-01-01T00:01:00Z",85]]}]}]}`,
		},
		&Query{
			name:    "spread - time",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT time, spread(visibility) FROM air where time >= '2000-01-01T00:00:00Z' AND time <= '2000-01-01T00:01:29Z' group by time(30s)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","spread"],"values":[["2000-01-01T00:00:00Z",30],["2000-01-01T00:00:30Z",10],["2000-01-01T00:01:00Z",85]]}]}]}`,
		},
		&Query{
			name:    "spread - pressure",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT pressure, spread(visibility) FROM air where time >= '2000-01-01T00:00:00Z' AND time <= '2000-01-01T00:01:29Z' group by time(30s)`,
			exp:     `{"results":[{"statement_id":0,"error":"mixing aggregate and non-aggregate queries is not supported"}]}`,
		},
		&Query{
			name:    "stddev - baseline 30s",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT stddev(visibility) FROM air where time >= '2000-01-01T00:00:00Z' AND time <= '2000-01-01T00:01:29Z' group by time(30s)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","stddev"],"values":[["2000-01-01T00:00:00Z",17.320508075688775],["2000-01-01T00:00:30Z",5.773502691896258],["2000-01-01T00:01:00Z",44.44097208657794]]}]}]}`,
		},
		&Query{
			name:    "stddev - time",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT time, stddev(visibility) FROM air where time >= '2000-01-01T00:00:00Z' AND time <= '2000-01-01T00:01:29Z' group by time(30s)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","stddev"],"values":[["2000-01-01T00:00:00Z",17.320508075688775],["2000-01-01T00:00:30Z",5.773502691896258],["2000-01-01T00:01:00Z",44.44097208657794]]}]}]}`,
		},
		&Query{
			name:    "stddev - pressure",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT pressure, stddev(visibility) FROM air where time >= '2000-01-01T00:00:00Z' AND time <= '2000-01-01T00:01:29Z' group by time(30s)`,
			exp:     `{"results":[{"statement_id":0,"error":"mixing aggregate and non-aggregate queries is not supported"}]}`,
		},
		&Query{
			name:    "percentile - baseline 30s",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT percentile(visibility, 75) FROM air where time >= '2000-01-01T00:00:00Z' AND time <= '2000-01-01T00:01:29Z' group by time(30s)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","percentile"],"values":[["2000-01-01T00:00:00Z",40],["2000-01-01T00:00:30Z",50],["2000-01-01T00:01:00Z",70]]}]}]}`,
		},
		&Query{
			name:    "percentile - time",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT time, percentile(visibility, 75) FROM air where time >= '2000-01-01T00:00:00Z' AND time <= '2000-01-01T00:01:29Z' group by time(30s)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","percentile"],"values":[["2000-01-01T00:00:00Z",40],["2000-01-01T00:00:30Z",50],["2000-01-01T00:01:00Z",70]]}]}]}`,
		},
		&Query{
			name:    "percentile - pressure",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT pressure, percentile(visibility, 75) FROM air where time >= '2000-01-01T00:00:00Z' AND time <= '2000-01-01T00:01:29Z' group by time(30s)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","pressure","percentile"],"values":[["2000-01-01T00:00:00Z",50,40],["2000-01-01T00:00:30Z",70,50],["2000-01-01T00:01:00Z",30,70]]}]}]}`,
		},
	}...)

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





