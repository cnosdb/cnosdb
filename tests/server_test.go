package tests

import (
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cnosdb/cnosdb/pkg/logger"
	"github.com/cnosdb/cnosdb/server/coordinator"
	"github.com/cnosdb/cnosdb/vend/db/models"
	"github.com/cnosdb/cnosdb/vend/db/tsdb"
	"go.uber.org/zap/zapcore"
)

// Global server used by benchmarks
var benchServer Server

func TestMain(m *testing.M) {
	flag.BoolVar(&VerboseServerLogs, "vv", false, "Turn on very verbose server logging.")
	flag.BoolVar(&CleanupData, "clean", true, "Clean up test data on disk.")
	flag.Int64Var(&Seed, "seed", 0, "Set specific seed controlling randomness.")
	flag.Parse()

	// Set random seed if not explicitly set.
	if Seed == 0 {
		Seed = time.Now().UnixNano()
	}
	rand.Seed(Seed)

	var r int
	for _, IndexType = range tsdb.RegisteredIndexes() {
		//setup server
		c := NewConfig()
		c.RetentionPolicy.Enabled = false
		c.Monitor.StoreEnabled = false
		c.Subscriber.Enabled = false
		c.ContinuousQuery.Enabled = true
		c.Data.MaxValuesPerTag = 1000000 // 1M
		c.Data.Index = IndexType
		c.Log = logger.NewDefaultLogConfig()
		c.Log.Level = zapcore.ErrorLevel
		c.HTTPD.MaxBodySize = 0
		if err := logger.InitZapLogger(c.Log); err != nil {
			fmt.Printf("parse log config: %s\n", err)
		}
		benchServer = OpenDefaultServer(c)

		if testing.Verbose() {
			fmt.Printf("================ Running all tests #{%v} index ================\n", IndexType)
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
			exp:     `{"results":[{"statement_id":0,"error":"expect time() or tag after GROUP BY"}]}`,
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
			fmt.Sprintf(`floatmany,station=XiaoMaiDao01 value=2.0 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00Z").UnixNano()),
			fmt.Sprintf(`floatmany,station=XiaoMaiDao02 value=4.0 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:10Z").UnixNano()),
			fmt.Sprintf(`floatmany,station=XiaoMaiDao03 value=4.0 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:20Z").UnixNano()),
			fmt.Sprintf(`floatmany,station=XiaoMaiDao04 value=4.0 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:30Z").UnixNano()),
			fmt.Sprintf(`floatmany,station=XiaoMaiDao05 value=5.0 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:40Z").UnixNano()),
			fmt.Sprintf(`floatmany,station=XiaoMaiDao06 value=5.0 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:50Z").UnixNano()),
			fmt.Sprintf(`floatmany,station=XiaoMaiDao07 value=7.0 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:01:00Z").UnixNano()),
			fmt.Sprintf(`floatmany,station=XiaoMaiDao08 value=9.0 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:01:10Z").UnixNano()),
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
			fmt.Sprintf(`offset,region=us-east,station=XiaoMaiDaoA value=20.0 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00Z").UnixNano()),
			fmt.Sprintf(`offset,region=us-east,station=XiaoMaiDaoB value=30.0 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:10Z").UnixNano()),
			fmt.Sprintf(`offset,region=us-west,station=XiaoMaiDaoC value=100.0 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00Z").UnixNano()),
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
			fmt.Sprintf(`load,region=us-east,station=XiaoMaiDaoA value=20.0 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00Z").UnixNano()),
			fmt.Sprintf(`load,region=us-east,station=XiaoMaiDaoB value=30.0 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:10Z").UnixNano()),
			fmt.Sprintf(`load,region=us-west,station=XiaoMaiDaoC value=100.0 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00Z").UnixNano()),
		}, "\n")},
	}

	test.addQueries([]*Query{
		&Query{
			name:    "group by multiple dimensions",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT sum(value) FROM load GROUP BY region, station`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"load","tags":{"region":"us-east","station":"XiaoMaiDaoA"},"columns":["time","sum"],"values":[["1970-01-01T00:00:00Z",20]]},{"name":"load","tags":{"region":"us-east","station":"XiaoMaiDaoB"},"columns":["time","sum"],"values":[["1970-01-01T00:00:00Z",30]]},{"name":"load","tags":{"region":"us-west","station":"XiaoMaiDaoC"},"columns":["time","sum"],"values":[["1970-01-01T00:00:00Z",100]]}]}]}`,
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
		fmt.Sprintf(`air,station=XiaoMaiDao01,region=west,moisture=1 visibility=10i,pressure=20i,moisture=2i %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00Z").UnixNano()),
		fmt.Sprintf(`air,station=XiaoMaiDao02,region=west,moisture=2 visibility=40i,pressure=50i,moisture=3i %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:10Z").UnixNano()),
		fmt.Sprintf(`air,station=XiaoMaiDao03,region=east,moisture=3 visibility=40i,pressure=55i,moisture=4i %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:20Z").UnixNano()),
		fmt.Sprintf(`air,station=XiaoMaiDao04,region=east,moisture=4 visibility=40i,pressure=60i,moisture=1i %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:30Z").UnixNano()),
		fmt.Sprintf(`air,station=XiaoMaiDao05,region=west,moisture=1 visibility=50i,pressure=70i,moisture=2i %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:40Z").UnixNano()),
		fmt.Sprintf(`air,station=XiaoMaiDao06,region=east,moisture=2 visibility=50i,pressure=40i,moisture=3i %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:50Z").UnixNano()),
		fmt.Sprintf(`air,station=XiaoMaiDao07,region=west,moisture=3 visibility=70i,pressure=30i,moisture=4i %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:01:00Z").UnixNano()),
		fmt.Sprintf(`air,station=XiaoMaiDao08,region=east,moisture=4 visibility=90i,pressure=10i,moisture=1i %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:01:10Z").UnixNano()),
		fmt.Sprintf(`air,station=XiaoMaiDao09,region=east,moisture=1 visibility=5i,pressure=4i,moisture=2i %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:01:20Z").UnixNano()),
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
		fmt.Sprintf(`air,station=XiaoMaiDao01,region=west,moisture=1 visibility=10i,pressure=20i,moisture=2i %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00Z").UnixNano()),
		fmt.Sprintf(`air,station=XiaoMaiDao02,region=west,moisture=2 visibility=40i,pressure=50i,moisture=3i %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:10Z").UnixNano()),
		fmt.Sprintf(`air,station=XiaoMaiDao03,region=east,moisture=3 visibility=40i,pressure=55i,moisture=4i %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:20Z").UnixNano()),
		fmt.Sprintf(`air,station=XiaoMaiDao04,region=east,moisture=4 visibility=40i,pressure=60i,moisture=1i %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:30Z").UnixNano()),
		fmt.Sprintf(`air,station=XiaoMaiDao05,region=west,moisture=1 visibility=50i,pressure=70i,moisture=2i %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:40Z").UnixNano()),
		fmt.Sprintf(`air,station=XiaoMaiDao06,region=east,moisture=2 visibility=50i,pressure=40i,moisture=3i %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:50Z").UnixNano()),
		fmt.Sprintf(`air,station=XiaoMaiDao07,region=west,moisture=3 visibility=70i,pressure=30i,moisture=4i %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:01:00Z").UnixNano()),
		fmt.Sprintf(`air,station=XiaoMaiDao08,region=east,moisture=4 visibility=90i,pressure=10i,moisture=1i %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:01:10Z").UnixNano()),
		fmt.Sprintf(`air,station=XiaoMaiDao09,region=east,moisture=1 visibility=5i,pressure=4i,moisture=2i %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:01:20Z").UnixNano()),
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
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","moisture","moisture_1","pressure","region","station","visibility"],"values":[["2000-01-01T00:00:00Z",2,"1",20,"west","XiaoMaiDao01",10],["2000-01-01T00:00:10Z",3,"2",50,"west","XiaoMaiDao02",40],["2000-01-01T00:00:20Z",4,"3",55,"east","XiaoMaiDao03",40],["2000-01-01T00:00:30Z",1,"4",60,"east","XiaoMaiDao04",40],["2000-01-01T00:00:40Z",2,"1",70,"west","XiaoMaiDao05",50],["2000-01-01T00:00:50Z",3,"2",40,"east","XiaoMaiDao06",50],["2000-01-01T00:01:00Z",4,"3",30,"west","XiaoMaiDao07",70],["2000-01-01T00:01:10Z",1,"4",10,"east","XiaoMaiDao08",90],["2000-01-01T00:01:20Z",2,"1",4,"east","XiaoMaiDao09",5]]}]}]}`,
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

func TestServer_Query_ExactTimeRange(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig())
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("rp0", 1, 0), true); err != nil {
		t.Fatal(err)
	}

	writes := []string{
		fmt.Sprintf(`air temperature=1 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00.000000000Z").UnixNano()),
		fmt.Sprintf(`air temperature=2 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00.000000001Z").UnixNano()),
		fmt.Sprintf(`air temperature=3 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00.000000002Z").UnixNano()),
	}

	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}

	test.addQueries([]*Query{
		&Query{
			name:    "query point at exactly one time - rfc3339nano",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT * FROM air WHERE time = '2000-01-01T00:00:00.000000001Z'`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","temperature"],"values":[["2000-01-01T00:00:00.000000001Z",2]]}]}]}`,
		},
		&Query{
			name:    "query point at exactly one time - timestamp",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT * FROM air WHERE time = 946684800000000001`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","temperature"],"values":[["2000-01-01T00:00:00.000000001Z",2]]}]}]}`,
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

func TestServer_Query_Selectors(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig())
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("rp0", 1, 0), true); err != nil {
		t.Fatal(err)
	}

	writes := []string{
		fmt.Sprintf(`air,station=XiaoMaiDoa01,region=west,moisture=1 visibility=10i,pressure=20i,moisture=2i %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00Z").UnixNano()),
		fmt.Sprintf(`air,station=XiaoMaiDoa02,region=west,moisture=2 visibility=40i,pressure=50i,moisture=3i %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:10Z").UnixNano()),
		fmt.Sprintf(`air,station=XiaoMaiDoa03,region=east,moisture=3 visibility=40i,pressure=55i,moisture=4i %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:20Z").UnixNano()),
		fmt.Sprintf(`air,station=XiaoMaiDoa04,region=east,moisture=4 visibility=40i,pressure=60i,moisture=1i %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:30Z").UnixNano()),
		fmt.Sprintf(`air,station=XiaoMaiDoa05,region=west,moisture=1 visibility=50i,pressure=70i,moisture=2i %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:40Z").UnixNano()),
		fmt.Sprintf(`air,station=XiaoMaiDoa06,region=east,moisture=2 visibility=50i,pressure=40i,moisture=3i %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:50Z").UnixNano()),
		fmt.Sprintf(`air,station=XiaoMaiDoa07,region=west,moisture=3 visibility=70i,pressure=30i,moisture=4i %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:01:00Z").UnixNano()),
		fmt.Sprintf(`air,station=XiaoMaiDoa08,region=east,moisture=4 visibility=90i,pressure=10i,moisture=1i %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:01:10Z").UnixNano()),
		fmt.Sprintf(`air,station=XiaoMaiDoa09,region=east,moisture=1 visibility=5i,pressure=4i,moisture=2i %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:01:20Z").UnixNano()),
	}

	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}

	test.addQueries([]*Query{
		&Query{
			name:    "max - pressure",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT max(pressure) FROM air`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","max"],"values":[["2000-01-01T00:00:40Z",70]]}]}]}`,
		},
		&Query{
			name:    "min - pressure",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT min(pressure) FROM air`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","min"],"values":[["2000-01-01T00:01:20Z",4]]}]}]}`,
		},
		&Query{
			name:    "first",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT first(pressure) FROM air`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","first"],"values":[["2000-01-01T00:00:00Z",20]]}]}]}`,
		},
		&Query{
			name:    "last",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT last(pressure) FROM air`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","last"],"values":[["2000-01-01T00:01:20Z",4]]}]}]}`,
		},
		&Query{
			name:    "percentile",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT percentile(pressure, 50) FROM air`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","percentile"],"values":[["2000-01-01T00:00:50Z",40]]}]}]}`,
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

func TestServer_Query_TopBottomInt(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig())
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("rp0", 1, 0), true); err != nil {
		t.Fatal(err)
	}

	writes := []string{
		// air data with overlapping duplicate values
		// hour 0
		fmt.Sprintf(`air,station=XiaoMaiDoa01 temperature=2.0 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00Z").UnixNano()),
		fmt.Sprintf(`air,station=XiaoMaiDoa02 temperature=3.0 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:10Z").UnixNano()),
		fmt.Sprintf(`air,station=XiaoMaiDoa03 temperature=4.0 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:20Z").UnixNano()),
		// hour 1
		fmt.Sprintf(`air,station=XiaoMaiDoa04 temperature=3.0 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T01:00:00Z").UnixNano()),
		fmt.Sprintf(`air,station=XiaoMaiDoa05 temperature=7.0 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T01:00:10Z").UnixNano()),
		fmt.Sprintf(`air,station=XiaoMaiDoa06 temperature=6.0 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T01:00:20Z").UnixNano()),
		// hour 2
		fmt.Sprintf(`air,station=XiaoMaiDoa07 temperature=7.0 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T02:00:00Z").UnixNano()),
		fmt.Sprintf(`air,station=XiaoMaiDoa08 temperature=9.0 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T02:00:10Z").UnixNano()),

		// sea data
		// hour 0
		fmt.Sprintf(`sea,station=a,service=Pacific temperature=1000i %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00Z").UnixNano()),
		fmt.Sprintf(`sea,station=b,service=Atlantic temperature=2000i %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00Z").UnixNano()),
		fmt.Sprintf(`sea,station=b,service=Pacific temperature=1500i %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00Z").UnixNano()),
		// hour 1
		fmt.Sprintf(`sea,station=a,service=Pacific temperature=1001i %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T01:00:00Z").UnixNano()),
		fmt.Sprintf(`sea,station=b,service=Atlantic temperature=2001i %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T01:00:00Z").UnixNano()),
		fmt.Sprintf(`sea,station=b,service=Pacific temperature=1501i %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T01:00:00Z").UnixNano()),
		// hour 2
		fmt.Sprintf(`sea,station=a,service=Pacific temperature=1002i %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T02:00:00Z").UnixNano()),
		fmt.Sprintf(`sea,station=b,service=Atlantic temperature=2002i %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T02:00:00Z").UnixNano()),
		fmt.Sprintf(`sea,station=b,service=Pacific temperature=1502i %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T02:00:00Z").UnixNano()),
	}

	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}

	test.addQueries([]*Query{
		&Query{
			name:    "top - air",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT TOP(temperature, 1) FROM air`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","top"],"values":[["2000-01-01T02:00:10Z",9]]}]}]}`,
		},
		&Query{
			name:    "bottom - air",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT BOTTOM(temperature, 1) FROM air`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","bottom"],"values":[["2000-01-01T00:00:00Z",2]]}]}]}`,
		},
		&Query{
			name:    "top - air - 2 temperatures",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT TOP(temperature, 2) FROM air`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","top"],"values":[["2000-01-01T01:00:10Z",7],["2000-01-01T02:00:10Z",9]]}]}]}`,
		},
		&Query{
			name:    "bottom - air - 2 temperatures",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT BOTTOM(temperature, 2) FROM air`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","bottom"],"values":[["2000-01-01T00:00:00Z",2],["2000-01-01T00:00:10Z",3]]}]}]}`,
		},
		&Query{
			name:    "top - air - 3 temperatures - sorts on tie properly",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT TOP(temperature, 3) FROM air`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","top"],"values":[["2000-01-01T01:00:10Z",7],["2000-01-01T02:00:00Z",7],["2000-01-01T02:00:10Z",9]]}]}]}`,
		},
		&Query{
			name:    "bottom - air - 3 temperatures - sorts on tie properly",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT BOTTOM(temperature, 3) FROM air`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","bottom"],"values":[["2000-01-01T00:00:00Z",2],["2000-01-01T00:00:10Z",3],["2000-01-01T01:00:00Z",3]]}]}]}`,
		},
		&Query{
			name:    "top - air - with tag",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT TOP(temperature, station, 2) FROM air`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","top","station"],"values":[["2000-01-01T01:00:10Z",7,"XiaoMaiDoa05"],["2000-01-01T02:00:10Z",9,"XiaoMaiDoa08"]]}]}]}`,
		},
		&Query{
			name:    "bottom - air - with tag",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT BOTTOM(temperature, station, 2) FROM air`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","bottom","station"],"values":[["2000-01-01T00:00:00Z",2,"XiaoMaiDoa01"],["2000-01-01T00:00:10Z",3,"XiaoMaiDoa02"]]}]}]}`,
		},
		&Query{
			name:    "top - air - 3 temperatures with limit 2",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT TOP(temperature, 3) FROM air limit 2`,
			exp:     `{"results":[{"statement_id":0,"error":"limit (3) in top function can not be larger than the LIMIT (2) in the select statement"}]}`,
		},
		&Query{
			name:    "bottom - air - 3 temperatures with limit 2",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT BOTTOM(temperature, 3) FROM air limit 2`,
			exp:     `{"results":[{"statement_id":0,"error":"limit (3) in bottom function can not be larger than the LIMIT (2) in the select statement"}]}`,
		},
		&Query{
			name:    "top - air - hourly",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT TOP(temperature, 1) FROM air where time >= '2000-01-01T00:00:00Z' and time <= '2000-01-01T02:00:10Z' group by time(1h)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","top"],"values":[["2000-01-01T00:00:20Z",4],["2000-01-01T01:00:10Z",7],["2000-01-01T02:00:10Z",9]]}]}]}`,
		},
		&Query{
			name:    "bottom - air - hourly",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT BOTTOM(temperature, 1) FROM air where time >= '2000-01-01T00:00:00Z' and time <= '2000-01-01T02:00:10Z' group by time(1h)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","bottom"],"values":[["2000-01-01T00:00:00Z",2],["2000-01-01T01:00:00Z",3],["2000-01-01T02:00:00Z",7]]}]}]}`,
		},
		&Query{
			name:    "top - air - 2 temperatures hourly",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT TOP(temperature, 2) FROM air where time >= '2000-01-01T00:00:00Z' and time <= '2000-01-01T02:00:10Z' group by time(1h)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","top"],"values":[["2000-01-01T00:00:10Z",3],["2000-01-01T00:00:20Z",4],["2000-01-01T01:00:10Z",7],["2000-01-01T01:00:20Z",6],["2000-01-01T02:00:00Z",7],["2000-01-01T02:00:10Z",9]]}]}]}`,
		},
		&Query{
			name:    "bottom - air - 2 temperatures hourly",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT BOTTOM(temperature, 2) FROM air where time >= '2000-01-01T00:00:00Z' and time <= '2000-01-01T02:00:10Z' group by time(1h)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","bottom"],"values":[["2000-01-01T00:00:00Z",2],["2000-01-01T00:00:10Z",3],["2000-01-01T01:00:00Z",3],["2000-01-01T01:00:20Z",6],["2000-01-01T02:00:00Z",7],["2000-01-01T02:00:10Z",9]]}]}]}`,
		},
		&Query{
			name:    "top - air - 3 temperatures hourly - validates that a bucket can have less than limit if no temperatures exist in that time bucket",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT TOP(temperature, 3) FROM air where time >= '2000-01-01T00:00:00Z' and time <= '2000-01-01T02:00:10Z' group by time(1h)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","top"],"values":[["2000-01-01T00:00:00Z",2],["2000-01-01T00:00:10Z",3],["2000-01-01T00:00:20Z",4],["2000-01-01T01:00:00Z",3],["2000-01-01T01:00:10Z",7],["2000-01-01T01:00:20Z",6],["2000-01-01T02:00:00Z",7],["2000-01-01T02:00:10Z",9]]}]}]}`,
		},
		&Query{
			name:    "bottom - air - 3 temperatures hourly - validates that a bucket can have less than limit if no temperatures exist in that time bucket",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT BOTTOM(temperature, 3) FROM air where time >= '2000-01-01T00:00:00Z' and time <= '2000-01-01T02:00:10Z' group by time(1h)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","bottom"],"values":[["2000-01-01T00:00:00Z",2],["2000-01-01T00:00:10Z",3],["2000-01-01T00:00:20Z",4],["2000-01-01T01:00:00Z",3],["2000-01-01T01:00:10Z",7],["2000-01-01T01:00:20Z",6],["2000-01-01T02:00:00Z",7],["2000-01-01T02:00:10Z",9]]}]}]}`,
		},
		&Query{
			name:    "top - sea - 2 temperatures, two tags",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT TOP(temperature, 2), station, service FROM sea`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"sea","columns":["time","top","station","service"],"values":[["2000-01-01T01:00:00Z",2001,"b","Atlantic"],["2000-01-01T02:00:00Z",2002,"b","Atlantic"]]}]}]}`,
		},
		&Query{
			name:    "bottom - sea - 2 temperatures, two tags",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT BOTTOM(temperature, 2), station, service FROM sea`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"sea","columns":["time","bottom","station","service"],"values":[["2000-01-01T00:00:00Z",1000,"a","Pacific"],["2000-01-01T01:00:00Z",1001,"a","Pacific"]]}]}]}`,
		},
		&Query{
			name:    "top - sea - station tag with limit 2",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT TOP(temperature, station, 2) FROM sea`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"sea","columns":["time","top","station"],"values":[["2000-01-01T02:00:00Z",2002,"b"],["2000-01-01T02:00:00Z",1002,"a"]]}]}]}`,
		},
		&Query{
			name:    "bottom - sea - station tag with limit 2",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT BOTTOM(temperature, station, 2) FROM sea`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"sea","columns":["time","bottom","station"],"values":[["2000-01-01T00:00:00Z",1000,"a"],["2000-01-01T00:00:00Z",1500,"b"]]}]}]}`,
		},
		&Query{
			name:    "top - sea - station tag with limit 2, service tag in select",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT TOP(temperature, station, 2), service FROM sea`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"sea","columns":["time","top","station","service"],"values":[["2000-01-01T02:00:00Z",2002,"b","Atlantic"],["2000-01-01T02:00:00Z",1002,"a","Pacific"]]}]}]}`,
		},
		&Query{
			name:    "bottom - sea - station tag with limit 2, service tag in select",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT BOTTOM(temperature, station, 2), service FROM sea`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"sea","columns":["time","bottom","station","service"],"values":[["2000-01-01T00:00:00Z",1000,"a","Pacific"],["2000-01-01T00:00:00Z",1500,"b","Pacific"]]}]}]}`,
		},
		&Query{
			name:    "top - sea - service tag with limit 2, station tag in select",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT TOP(temperature, service, 2), station FROM sea`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"sea","columns":["time","top","service","station"],"values":[["2000-01-01T02:00:00Z",2002,"Atlantic","b"],["2000-01-01T02:00:00Z",1502,"Pacific","b"]]}]}]}`,
		},
		&Query{
			name:    "bottom - sea - service tag with limit 2, station tag in select",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT BOTTOM(temperature, service, 2), station FROM sea`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"sea","columns":["time","bottom","service","station"],"values":[["2000-01-01T00:00:00Z",1000,"Pacific","a"],["2000-01-01T00:00:00Z",2000,"Atlantic","b"]]}]}]}`,
		},
		&Query{
			name:    "top - sea - station and service tag with limit 2",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT TOP(temperature, station, service, 2) FROM sea`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"sea","columns":["time","top","station","service"],"values":[["2000-01-01T02:00:00Z",2002,"b","Atlantic"],["2000-01-01T02:00:00Z",1502,"b","Pacific"]]}]}]}`,
		},
		&Query{
			name:    "bottom - sea - station and service tag with limit 2",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT BOTTOM(temperature, station, service, 2) FROM sea`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"sea","columns":["time","bottom","station","service"],"values":[["2000-01-01T00:00:00Z",1000,"a","Pacific"],["2000-01-01T00:00:00Z",1500,"b","Pacific"]]}]}]}`,
		},
		&Query{
			name:    "top - sea - station tag with limit 2 with service tag in select",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT TOP(temperature, station, 2), service FROM sea`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"sea","columns":["time","top","station","service"],"values":[["2000-01-01T02:00:00Z",2002,"b","Atlantic"],["2000-01-01T02:00:00Z",1002,"a","Pacific"]]}]}]}`,
		},
		&Query{
			name:    "bottom - sea - station tag with limit 2 with service tag in select",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT BOTTOM(temperature, station, 2), service FROM sea`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"sea","columns":["time","bottom","station","service"],"values":[["2000-01-01T00:00:00Z",1000,"a","Pacific"],["2000-01-01T00:00:00Z",1500,"b","Pacific"]]}]}]}`,
		},
		&Query{
			name:    "top - sea - station and service tag with limit 3",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT TOP(temperature, station, service, 3) FROM sea`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"sea","columns":["time","top","station","service"],"values":[["2000-01-01T02:00:00Z",2002,"b","Atlantic"],["2000-01-01T02:00:00Z",1502,"b","Pacific"],["2000-01-01T02:00:00Z",1002,"a","Pacific"]]}]}]}`,
		},
		&Query{
			name:    "bottom - sea - station and service tag with limit 3",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT BOTTOM(temperature, station, service, 3) FROM sea`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"sea","columns":["time","bottom","station","service"],"values":[["2000-01-01T00:00:00Z",1000,"a","Pacific"],["2000-01-01T00:00:00Z",1500,"b","Pacific"],["2000-01-01T00:00:00Z",2000,"b","Atlantic"]]}]}]}`,
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
				t.Skipf("SKIP: %s", query.name)
			}

			if err := query.Execute(s); err != nil {
				t.Error(query.Error(err))
			} else if !query.success() {
				t.Error(query.failureMessage())
			}
		})
	}
}

func TestServer_Query_TopBottomWriteTags(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig())
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("rp0", 1, 0), true); err != nil {
		t.Fatal(err)
	}

	writes := []string{
		fmt.Sprintf(`air,station=XiaoMaiDoa01 temperature=2.0 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00Z").UnixNano()),
		fmt.Sprintf(`air,station=XiaoMaiDoa02 temperature=3.0 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:10Z").UnixNano()),
		fmt.Sprintf(`air,station=XiaoMaiDoa03 temperature=4.0 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:20Z").UnixNano()),
		// hour 1
		fmt.Sprintf(`air,station=XiaoMaiDoa04 temperature=5.0 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T01:00:00Z").UnixNano()),
		fmt.Sprintf(`air,station=XiaoMaiDoa05 temperature=7.0 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T01:00:10Z").UnixNano()),
		fmt.Sprintf(`air,station=XiaoMaiDoa06 temperature=6.0 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T01:00:20Z").UnixNano()),
		// hour 2
		fmt.Sprintf(`air,station=XiaoMaiDoa07 temperature=7.0 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T02:00:00Z").UnixNano()),
		fmt.Sprintf(`air,station=XiaoMaiDoa08 temperature=9.0 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T02:00:10Z").UnixNano()),
	}

	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}

	test.addQueries([]*Query{
		&Query{
			name:    "top - write - with tag",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT top(temperature, station, 2) INTO air_top FROM air`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"result","columns":["time","written"],"values":[["1970-01-01T00:00:00Z",2]]}]}]}`,
		},
		&Query{
			name:    "top - read results with tags",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT * FROM air_top GROUP BY *`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air_top","tags":{"station":"XiaoMaiDoa05"},"columns":["time","top"],"values":[["2000-01-01T01:00:10Z",7]]},{"name":"air_top","tags":{"station":"XiaoMaiDoa08"},"columns":["time","top"],"values":[["2000-01-01T02:00:10Z",9]]}]}]}`,
		},
		&Query{
			name:    "top - read results as fields",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT * FROM air_top`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air_top","columns":["time","station","top"],"values":[["2000-01-01T01:00:10Z","XiaoMaiDoa05",7],["2000-01-01T02:00:10Z","XiaoMaiDoa08",9]]}]}]}`,
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
				t.Skipf("SKIP: %s", query.name)
			}

			if err := query.Execute(s); err != nil {
				t.Error(query.Error(err))
			} else if !query.success() {
				t.Error(query.failureMessage())
			}
		})
	}
}

// Test various aggregates when different series only have data for the same timestamp.
func TestServer_Query_Aggregates_IdenticalTime(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig())
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("rp0", 1, 0), true); err != nil {
		t.Fatal(err)
	}

	writes := []string{
		fmt.Sprintf(`series,station=a temperature=1 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00Z").UnixNano()),
		fmt.Sprintf(`series,station=b temperature=2 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00Z").UnixNano()),
		fmt.Sprintf(`series,station=c temperature=3 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00Z").UnixNano()),
		fmt.Sprintf(`series,station=d temperature=4 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00Z").UnixNano()),
		fmt.Sprintf(`series,station=e temperature=5 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00Z").UnixNano()),
		fmt.Sprintf(`series,station=f temperature=5 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00Z").UnixNano()),
		fmt.Sprintf(`series,station=g temperature=5 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00Z").UnixNano()),
		fmt.Sprintf(`series,station=h temperature=5 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00Z").UnixNano()),
		fmt.Sprintf(`series,station=i temperature=5 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00Z").UnixNano()),
	}

	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}

	test.addQueries([]*Query{
		&Query{
			name:    "last from multiple series with identical timestamp",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT last(temperature) FROM "series"`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"series","columns":["time","last"],"values":[["2000-01-01T00:00:00Z",5]]}]}]}`,
			repeat:  100,
		},
		&Query{
			name:    "first from multiple series with identical timestamp",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT first(temperature) FROM "series"`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"series","columns":["time","first"],"values":[["2000-01-01T00:00:00Z",5]]}]}]}`,
			repeat:  100,
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
			for n := 0; n <= query.repeat; n++ {
				if err := query.Execute(s); err != nil {
					t.Error(query.Error(err))
				} else if !query.success() {
					t.Error(query.failureMessage())
				}
			}
		})
	}
}

// This will test that when using a group by, that it observes the time you asked for
// but will only put the values in the bucket that match the time range
func TestServer_Query_GroupByTimeCutoffs(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig())
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("rp0", 1, 0), true); err != nil {
		t.Fatal(err)
	}

	writes := []string{
		fmt.Sprintf(`air temperature=1i %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00Z").UnixNano()),
		fmt.Sprintf(`air temperature=2i %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:01Z").UnixNano()),
		fmt.Sprintf(`air temperature=3i %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:05Z").UnixNano()),
		fmt.Sprintf(`air temperature=4i %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:08Z").UnixNano()),
		fmt.Sprintf(`air temperature=5i %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:09Z").UnixNano()),
		fmt.Sprintf(`air temperature=6i %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:10Z").UnixNano()),
	}
	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}

	test.addQueries([]*Query{
		&Query{
			name:    "sum all time",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT SUM(temperature) FROM air`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","sum"],"values":[["1970-01-01T00:00:00Z",21]]}]}]}`,
		},
		&Query{
			name:    "sum all time grouped by time 5s",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT SUM(temperature) FROM air where time >= '2000-01-01T00:00:00Z' and time <= '2000-01-01T00:00:10Z' group by time(5s)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","sum"],"values":[["2000-01-01T00:00:00Z",3],["2000-01-01T00:00:05Z",12],["2000-01-01T00:00:10Z",6]]}]}]}`,
		},
		&Query{
			name:    "sum all time grouped by time 5s missing first point",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT SUM(temperature) FROM air where time >= '2000-01-01T00:00:01Z' and time <= '2000-01-01T00:00:10Z' group by time(5s)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","sum"],"values":[["2000-01-01T00:00:00Z",2],["2000-01-01T00:00:05Z",12],["2000-01-01T00:00:10Z",6]]}]}]}`,
		},
		&Query{
			name:    "sum all time grouped by time 5s missing first points (null for bucket)",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT SUM(temperature) FROM air where time >= '2000-01-01T00:00:02Z' and time <= '2000-01-01T00:00:10Z' group by time(5s)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","sum"],"values":[["2000-01-01T00:00:00Z",null],["2000-01-01T00:00:05Z",12],["2000-01-01T00:00:10Z",6]]}]}]}`,
		},
		&Query{
			name:    "sum all time grouped by time 5s missing last point - 2 time intervals",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT SUM(temperature) FROM air where time >= '2000-01-01T00:00:00Z' and time <= '2000-01-01T00:00:09Z' group by time(5s)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","sum"],"values":[["2000-01-01T00:00:00Z",3],["2000-01-01T00:00:05Z",12]]}]}]}`,
		},
		&Query{
			name:    "sum all time grouped by time 5s missing last 2 points - 2 time intervals",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT SUM(temperature) FROM air where time >= '2000-01-01T00:00:00Z' and time <= '2000-01-01T00:00:08Z' group by time(5s)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","sum"],"values":[["2000-01-01T00:00:00Z",3],["2000-01-01T00:00:05Z",7]]}]}]}`,
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

func TestServer_Query_MapType(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig())
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("rp0", 1, 0), true); err != nil {
		t.Fatal(err)
	}

	writes := []string{
		fmt.Sprintf(`air temperature=2 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00Z").UnixNano()),
		fmt.Sprintf(`wind speed=25 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00Z").UnixNano()),
	}
	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}

	test.addQueries([]*Query{
		&Query{
			name:    "query value with a single measurement",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT temperature FROM air`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","temperature"],"values":[["2000-01-01T00:00:00Z",2]]}]}]}`,
		},
		&Query{
			name:    "query wildcard with a single measurement",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT * FROM air`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","temperature"],"values":[["2000-01-01T00:00:00Z",2]]}]}]}`,
		},
		&Query{
			name:    "query value with multiple measurements",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT temperature FROM air, wind`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","temperature"],"values":[["2000-01-01T00:00:00Z",2]]}]}]}`,
		},
		&Query{
			name:    "query wildcard with multiple measurements",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT * FROM air, wind`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","speed","temperature"],"values":[["2000-01-01T00:00:00Z",null,2]]},{"name":"wind","columns":["time","speed","temperature"],"values":[["2000-01-01T00:00:00Z",25,null]]}]}]}`,
		},
		&Query{
			name:    "query value with a regex measurement",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT temperature FROM /air|wind/`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","temperature"],"values":[["2000-01-01T00:00:00Z",2]]}]}]}`,
		},
		&Query{
			name:    "query wildcard with a regex measurement",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT * FROM /air|wind/`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","speed","temperature"],"values":[["2000-01-01T00:00:00Z",null,2]]},{"name":"wind","columns":["time","speed","temperature"],"values":[["2000-01-01T00:00:00Z",25,null]]}]}]}`,
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

func TestServer_Query_Subqueries(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig())
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("rp0", 1, 0), true); err != nil {
		t.Fatal(err)
	}

	writes := []string{
		fmt.Sprintf(`air,station=XiaoMaiDao01 visibility=70i,pressure=30i %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00Z").UnixNano()),
		fmt.Sprintf(`air,station=XiaoMaiDao01 visibility=45i,pressure=55i %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:10Z").UnixNano()),
		fmt.Sprintf(`air,station=XiaoMaiDao01 visibility=23i,pressure=77i %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:20Z").UnixNano()),
		fmt.Sprintf(`air,station=XiaoMaiDao02 visibility=11i,pressure=89i %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00Z").UnixNano()),
		fmt.Sprintf(`air,station=XiaoMaiDao02 visibility=28i,pressure=72i %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:10Z").UnixNano()),
		fmt.Sprintf(`air,station=XiaoMaiDao02 visibility=12i,pressure=53i %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:20Z").UnixNano()),
	}
	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}

	test.addQueries([]*Query{
		&Query{
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT mean FROM (SELECT mean(visibility) FROM air) WHERE time >= '2000-01-01T00:00:00Z' AND time < '2000-01-01T00:00:30Z'`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","mean"],"values":[["2000-01-01T00:00:00Z",31.5]]}]}]}`,
		},
		&Query{
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT temperature FROM (SELECT mean(visibility) AS temperature FROM air) WHERE time >= '2000-01-01T00:00:00Z' AND time < '2000-01-01T00:00:30Z'`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","temperature"],"values":[["2000-01-01T00:00:00Z",31.5]]}]}]}`,
		},
		&Query{
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT mean(usage) FROM (SELECT 100 - visibility AS usage FROM air) WHERE time >= '2000-01-01T00:00:00Z' AND time < '2000-01-01T00:00:30Z'`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","mean"],"values":[["2000-01-01T00:00:00Z",68.5]]}]}]}`,
		},
		&Query{
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT station FROM (SELECT min(visibility), station FROM air) WHERE time >= '2000-01-01T00:00:00Z' AND time < '2000-01-01T00:00:30Z'`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","station"],"values":[["2000-01-01T00:00:00Z","XiaoMaiDao02"]]}]}]}`,
		},
		&Query{
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT station FROM (SELECT min(visibility) FROM air GROUP BY station) WHERE time >= '2000-01-01T00:00:00Z' AND time < '2000-01-01T00:00:30Z'`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","station"],"values":[["2000-01-01T00:00:00Z","XiaoMaiDao02"],["2000-01-01T00:00:20Z","XiaoMaiDao01"]]}]}]}`,
		},
		&Query{
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT station FROM (SELECT min(visibility) FROM air GROUP BY station) WHERE time >= '2000-01-01T00:00:00Z' AND time < '2000-01-01T00:00:30Z' GROUP BY station`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","tags":{"station":"XiaoMaiDao01"},"columns":["time","station"],"values":[["2000-01-01T00:00:20Z","XiaoMaiDao01"]]},{"name":"air","tags":{"station":"XiaoMaiDao02"},"columns":["time","station"],"values":[["2000-01-01T00:00:00Z","XiaoMaiDao02"]]}]}]}`,
		},
		&Query{
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT mean(min) FROM (SELECT min(visibility) FROM air GROUP BY station) WHERE time >= '2000-01-01T00:00:00Z' AND time < '2000-01-01T00:00:30Z'`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","mean"],"values":[["2000-01-01T00:00:00Z",17]]}]}]}`,
		},
		&Query{
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT mean(min) FROM (SELECT (min(visibility)) FROM air GROUP BY station) WHERE time >= '2000-01-01T00:00:00Z' AND time < '2000-01-01T00:00:30Z'`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","mean"],"values":[["2000-01-01T00:00:00Z",17]]}]}]}`,
		},
		&Query{
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT max(min), station FROM (SELECT min(visibility) FROM air GROUP BY station) WHERE time >= '2000-01-01T00:00:00Z' AND time < '2000-01-01T00:00:30Z'`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","max","station"],"values":[["2000-01-01T00:00:20Z",23,"XiaoMaiDao01"]]}]}]}`,
		},
		&Query{
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT mean, station FROM (SELECT mean(visibility) FROM air GROUP BY station) WHERE time >= '2000-01-01T00:00:00Z' AND time < '2000-01-01T00:00:30Z'`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","mean","station"],"values":[["2000-01-01T00:00:00Z",46,"XiaoMaiDao01"],["2000-01-01T00:00:00Z",17,"XiaoMaiDao02"]]}]}]}`,
		},
		&Query{
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT station FROM (SELECT mean(visibility) FROM air GROUP BY station) WHERE time >= '2000-01-01T00:00:00Z' AND time < '2000-01-01T00:00:30Z'`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","station"],"values":[["2000-01-01T00:00:00Z","XiaoMaiDao01"],["2000-01-01T00:00:00Z","XiaoMaiDao02"]]}]}]}`,
		},
		&Query{
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT max(pressure) FROM (SELECT min(visibility), pressure FROM air GROUP BY station) WHERE time >= '2000-01-01T00:00:00Z' AND time < '2000-01-01T00:00:30Z'`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","max"],"values":[["2000-01-01T00:00:00Z",89]]}]}]}`,
		},
		&Query{
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT min(top), station FROM (SELECT top(visibility, station, 2) FROM air) WHERE time >= '2000-01-01T00:00:00Z' AND time < '2000-01-01T00:00:30Z'`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","min","station"],"values":[["2000-01-01T00:00:10Z",28,"XiaoMaiDao02"]]}]}]}`,
		},
		&Query{
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT min(top), station FROM (SELECT top(visibility, 2), station FROM air) WHERE time >= '2000-01-01T00:00:00Z' AND time < '2000-01-01T00:00:30Z'`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","min","station"],"values":[["2000-01-01T00:00:10Z",45,"XiaoMaiDao01"]]}]}]}`,
		},
		&Query{
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT count(station) FROM (SELECT top(visibility, station, 2) FROM air) WHERE time >= '2000-01-01T00:00:00Z' AND time < '2000-01-01T00:00:30Z'`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","count"],"values":[["2000-01-01T00:00:00Z",2]]}]}]}`,
		},
		&Query{
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT sum(derivative) FROM (SELECT derivative(visibility) FROM air GROUP BY station) WHERE time >= '2000-01-01T00:00:00Z' AND time < '2000-01-01T00:00:30Z'`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","sum"],"values":[["2000-01-01T00:00:00Z",-4.6]]}]}]}`,
		},
		&Query{
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT min(max) FROM (SELECT 100 - max(visibility) FROM air GROUP BY station) WHERE time >= '2000-01-01T00:00:00Z' AND time < '2000-01-01T00:00:30Z'`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","min"],"values":[["2000-01-01T00:00:00Z",30]]}]}]}`,
		},
		&Query{
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT min(pressure) FROM (SELECT max(visibility), 100 - pressure FROM air GROUP BY station) WHERE time >= '2000-01-01T00:00:00Z' AND time < '2000-01-01T00:00:30Z'`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","min"],"values":[["2000-01-01T00:00:10Z",28]]}]}]}`,
		},
		&Query{
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT min(temperature) FROM (SELECT max(visibility), visibility - pressure AS temperature FROM air GROUP BY station) WHERE time >= '2000-01-01T00:00:00Z' AND time < '2000-01-01T00:00:30Z'`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","min"],"values":[["2000-01-01T00:00:10Z",-44]]}]}]}`,
		},
		&Query{
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT min(temperature) FROM (SELECT top(visibility, 2), visibility - pressure AS temperature FROM air) WHERE time >= '2000-01-01T00:00:00Z' AND time < '2000-01-01T00:00:30Z' GROUP BY station`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","tags":{"station":"XiaoMaiDao01"},"columns":["time","min"],"values":[["2000-01-01T00:00:10Z",-10]]},{"name":"air","tags":{"station":"XiaoMaiDao02"},"columns":["time","min"],"values":[["2000-01-01T00:00:10Z",-44]]}]}]}`,
		},
		&Query{
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT min(temperature) FROM (SELECT max(visibility), visibility - pressure AS temperature FROM air GROUP BY station) WHERE time >= '2000-01-01T00:00:00Z' AND time < '2000-01-01T00:00:30Z' AND station = 'XiaoMaiDao01'`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","min"],"values":[["2000-01-01T00:00:00Z",40]]}]}]}`,
		},
		&Query{
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT temperature FROM (SELECT max(visibility), visibility - pressure AS temperature FROM air GROUP BY station) WHERE time >= '2000-01-01T00:00:00Z' AND time < '2000-01-01T00:00:30Z' AND temperature > 0`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","temperature"],"values":[["2000-01-01T00:00:00Z",40]]}]}]}`,
		},
		&Query{
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT max FROM (SELECT max(visibility) FROM air GROUP BY station) WHERE time >= '2000-01-01T00:00:00Z' AND time < '2000-01-01T00:00:30Z' AND station = 'XiaoMaiDao01'`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","max"],"values":[["2000-01-01T00:00:00Z",70]]}]}]}`,
		},
		&Query{
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT mean(temperature) FROM (SELECT max(visibility), visibility - pressure AS temperature FROM air GROUP BY station) WHERE time >= '2000-01-01T00:00:00Z' AND time < '2000-01-01T00:00:30Z' AND temperature > 0`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","mean"],"values":[["2000-01-01T00:00:00Z",40]]}]}]}`,
		},
		&Query{
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT mean(temperature) FROM (SELECT max(visibility), visibility - pressure AS temperature FROM air GROUP BY station) WHERE time >= '2000-01-01T00:00:00Z' AND time < '2000-01-01T00:00:30Z' AND station =~ /XiaoMaiDao/`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","mean"],"values":[["2000-01-01T00:00:00Z",-2]]}]}]}`,
		},
		&Query{
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT top(pressure, station, 2) FROM (SELECT min(visibility), pressure FROM air GROUP BY time(20s), station) WHERE time >= '2000-01-01T00:00:00Z' AND time < '2000-01-01T00:00:30Z'`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","top","station"],"values":[["2000-01-01T00:00:00Z",89,"XiaoMaiDao02"],["2000-01-01T00:00:20Z",77,"XiaoMaiDao01"]]}]}]}`,
		},
		&Query{
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT bottom(pressure, station, 2) FROM (SELECT max(visibility), pressure FROM air GROUP BY time(20s), station) WHERE time >= '2000-01-01T00:00:00Z' AND time < '2000-01-01T00:00:30Z'`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","bottom","station"],"values":[["2000-01-01T00:00:00Z",30,"XiaoMaiDao01"],["2000-01-01T00:00:20Z",53,"XiaoMaiDao02"]]}]}]}`,
		},
	}...)
}

func TestServer_Query_SubqueryWithGroupBy(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig())
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("rp0", 1, 0), true); err != nil {
		t.Fatal(err)
	}

	writes := []string{
		fmt.Sprintf(`air,station=XiaoMaiDao01,region=beijing temperature=1i %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00Z").UnixNano()),
		fmt.Sprintf(`air,station=XiaoMaiDao01,region=beijing temperature=2i %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:01Z").UnixNano()),
		fmt.Sprintf(`air,station=XiaoMaiDao01,region=beijing temperature=3i %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:02Z").UnixNano()),
		fmt.Sprintf(`air,station=XiaoMaiDao01,region=beijing temperature=4i %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:03Z").UnixNano()),
		fmt.Sprintf(`air,station=XiaoMaiDao02,region=beijing temperature=5i %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00Z").UnixNano()),
		fmt.Sprintf(`air,station=XiaoMaiDao02,region=beijing temperature=6i %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:01Z").UnixNano()),
		fmt.Sprintf(`air,station=XiaoMaiDao02,region=beijing temperature=7i %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:02Z").UnixNano()),
		fmt.Sprintf(`air,station=XiaoMaiDao02,region=beijing temperature=8i %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:03Z").UnixNano()),
		fmt.Sprintf(`air,station=XiaoMaiDao01,region=haidian temperature=9i %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00Z").UnixNano()),
		fmt.Sprintf(`air,station=XiaoMaiDao01,region=haidian temperature=10i %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:01Z").UnixNano()),
		fmt.Sprintf(`air,station=XiaoMaiDao01,region=haidian temperature=11i %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:02Z").UnixNano()),
		fmt.Sprintf(`air,station=XiaoMaiDao01,region=haidian temperature=12i %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:03Z").UnixNano()),
		fmt.Sprintf(`air,station=XiaoMaiDao02,region=haidian temperature=13i %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00Z").UnixNano()),
		fmt.Sprintf(`air,station=XiaoMaiDao02,region=haidian temperature=14i %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:01Z").UnixNano()),
		fmt.Sprintf(`air,station=XiaoMaiDao02,region=haidian temperature=15i %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:02Z").UnixNano()),
		fmt.Sprintf(`air,station=XiaoMaiDao02,region=haidian temperature=16i %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:03Z").UnixNano()),
	}
	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}

	test.addQueries([]*Query{
		&Query{
			name:    "group by time(2s) - time(2s), station",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT mean(mean) FROM (SELECT mean(temperature) FROM air GROUP BY time(2s), station) WHERE time >= '2000-01-01T00:00:00Z' AND time < '2000-01-01T00:00:04Z' GROUP BY time(2s)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","mean"],"values":[["2000-01-01T00:00:00Z",7.5],["2000-01-01T00:00:02Z",9.5]]}]}]}`,
		},
		&Query{
			name:    "group by time(4s), station - time(2s), station",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT mean(mean) FROM (SELECT mean(temperature) FROM air GROUP BY time(2s), station) WHERE time >= '2000-01-01T00:00:00Z' AND time < '2000-01-01T00:00:04Z' GROUP BY time(4s), station`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","tags":{"station":"XiaoMaiDao01"},"columns":["time","mean"],"values":[["2000-01-01T00:00:00Z",6.5]]},{"name":"air","tags":{"station":"XiaoMaiDao02"},"columns":["time","mean"],"values":[["2000-01-01T00:00:00Z",10.5]]}]}]}`,
		},
		&Query{
			name:    "group by time(2s), station - time(2s), station, region",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT mean(mean) FROM (SELECT mean(temperature) FROM air GROUP BY time(2s), station, region) WHERE time >= '2000-01-01T00:00:00Z' AND time < '2000-01-01T00:00:04Z' GROUP BY time(2s), station`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","tags":{"station":"XiaoMaiDao01"},"columns":["time","mean"],"values":[["2000-01-01T00:00:00Z",5.5],["2000-01-01T00:00:02Z",7.5]]},{"name":"air","tags":{"station":"XiaoMaiDao02"},"columns":["time","mean"],"values":[["2000-01-01T00:00:00Z",9.5],["2000-01-01T00:00:02Z",11.5]]}]}]}`,
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

func TestServer_Query_SubqueryMath(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig())
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("rp0", 1, 0), true); err != nil {
		t.Fatal(err)
	}

	writes := []string{
		fmt.Sprintf("m0 f2=4,f3=2 %d", mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00Z").UnixNano()),
		fmt.Sprintf("m0 f1=5,f3=8 %d", mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:10Z").UnixNano()),
		fmt.Sprintf("m0 f1=5,f2=3,f3=6 %d", mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:20Z").UnixNano()),
	}
	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}

	test.addQueries([]*Query{
		&Query{
			name:    "SumThreeValues",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT sum FROM (SELECT f1 + f2 + f3 AS sum FROM m0)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"m0","columns":["time","sum"],"values":[["2000-01-01T00:00:00Z",null],["2000-01-01T00:00:10Z",null],["2000-01-01T00:00:20Z",14]]}]}]}`,
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

func TestServer_Query_PercentileDerivative(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig())
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("rp0", 1, 0), true); err != nil {
		t.Fatal(err)
	}

	writes := []string{
		fmt.Sprintf(`air temperature=12 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00Z").UnixNano()),
		fmt.Sprintf(`air temperature=34 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:10Z").UnixNano()),
		fmt.Sprintf(`air temperature=78 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:20Z").UnixNano()),
		fmt.Sprintf(`air temperature=89 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:30Z").UnixNano()),
		fmt.Sprintf(`air temperature=101 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:40Z").UnixNano()),
	}
	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}

	test.addQueries([]*Query{
		&Query{
			name:    "nth percentile of derivative",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT percentile(derivative, 95) FROM (SELECT derivative(temperature, 1s) FROM air) WHERE time >= '2000-01-01T00:00:00Z' AND time < '2000-01-01T00:00:50Z'`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","percentile"],"values":[["2000-01-01T00:00:20Z",4.4]]}]}]}`,
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

func TestServer_Query_UnderscoreMeasurement(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig())
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("rp0", 1, 0), true); err != nil {
		t.Fatal(err)
	}

	writes := []string{
		fmt.Sprintf(`_air temperature=1i %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00Z").UnixNano()),
	}
	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}

	test.addQueries([]*Query{
		&Query{
			name:    "select underscore with underscore prefix",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT * FROM _air`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"_air","columns":["time","temperature"],"values":[["2000-01-01T00:00:00Z",1]]}]}]}`,
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

func TestServer_Write_Precision(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig())
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("rp0", 1, 0), true); err != nil {
		t.Fatal(err)
	}

	writes := []struct {
		write  string
		params url.Values
	}{
		{
			write: fmt.Sprintf("air_n0_precision temperature=1 %d", mustParseTime(time.RFC3339Nano, "2000-01-01T12:34:56.789012345Z").UnixNano()),
		},
		{
			write:  fmt.Sprintf("air_n1_precision temperature=1.1 %d", mustParseTime(time.RFC3339Nano, "2000-01-01T12:34:56.789012345Z").UnixNano()),
			params: url.Values{"precision": []string{"n"}},
		},
		{
			write:  fmt.Sprintf("air_u_precision temperature=100 %d", mustParseTime(time.RFC3339Nano, "2000-01-01T12:34:56.789012345Z").Truncate(time.Microsecond).UnixNano()/int64(time.Microsecond)),
			params: url.Values{"precision": []string{"u"}},
		},
		{
			write:  fmt.Sprintf("air_ms_precision temperature=200 %d", mustParseTime(time.RFC3339Nano, "2000-01-01T12:34:56.789012345Z").Truncate(time.Millisecond).UnixNano()/int64(time.Millisecond)),
			params: url.Values{"precision": []string{"ms"}},
		},
		{
			write:  fmt.Sprintf("air_s_precision temperature=300 %d", mustParseTime(time.RFC3339Nano, "2000-01-01T12:34:56.789012345Z").Truncate(time.Second).UnixNano()/int64(time.Second)),
			params: url.Values{"precision": []string{"s"}},
		},
		{
			write:  fmt.Sprintf("air_m_precision temperature=400 %d", mustParseTime(time.RFC3339Nano, "2000-01-01T12:34:56.789012345Z").Truncate(time.Minute).UnixNano()/int64(time.Minute)),
			params: url.Values{"precision": []string{"m"}},
		},
		{
			write:  fmt.Sprintf("air_h_precision temperature=500 %d", mustParseTime(time.RFC3339Nano, "2000-01-01T12:34:56.789012345Z").Truncate(time.Hour).UnixNano()/int64(time.Hour)),
			params: url.Values{"precision": []string{"h"}},
		},
	}

	test := NewTest("db0", "rp0")

	test.addQueries([]*Query{
		&Query{
			name:    "point with nanosecond precision time - no precision specified on write",
			command: `SELECT * FROM air_n0_precision`,
			params:  url.Values{"db": []string{"db0"}},
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air_n0_precision","columns":["time","temperature"],"values":[["2000-01-01T12:34:56.789012345Z",1]]}]}]}`,
		},
		&Query{
			name:    "point with nanosecond precision time",
			command: `SELECT * FROM air_n1_precision`,
			params:  url.Values{"db": []string{"db0"}},
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air_n1_precision","columns":["time","temperature"],"values":[["2000-01-01T12:34:56.789012345Z",1.1]]}]}]}`,
		},
		&Query{
			name:    "point with microsecond precision time",
			command: `SELECT * FROM air_u_precision`,
			params:  url.Values{"db": []string{"db0"}},
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air_u_precision","columns":["time","temperature"],"values":[["2000-01-01T12:34:56.789012Z",100]]}]}]}`,
		},
		&Query{
			name:    "point with millisecond precision time",
			command: `SELECT * FROM air_ms_precision`,
			params:  url.Values{"db": []string{"db0"}},
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air_ms_precision","columns":["time","temperature"],"values":[["2000-01-01T12:34:56.789Z",200]]}]}]}`,
		},
		&Query{
			name:    "point with second precision time",
			command: `SELECT * FROM air_s_precision`,
			params:  url.Values{"db": []string{"db0"}},
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air_s_precision","columns":["time","temperature"],"values":[["2000-01-01T12:34:56Z",300]]}]}]}`,
		},
		&Query{
			name:    "point with minute precision time",
			command: `SELECT * FROM air_m_precision`,
			params:  url.Values{"db": []string{"db0"}},
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air_m_precision","columns":["time","temperature"],"values":[["2000-01-01T12:34:00Z",400]]}]}]}`,
		},
		&Query{
			name:    "point with hour precision time",
			command: `SELECT * FROM air_h_precision`,
			params:  url.Values{"db": []string{"db0"}},
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air_h_precision","columns":["time","temperature"],"values":[["2000-01-01T12:00:00Z",500]]}]}]}`,
		},
	}...)

	// we are doing writes that require parameter changes, so we are fighting the test harness a little to make this happen properly
	for _, w := range writes {
		test.writes = Writes{
			&Write{data: w.write},
		}
		test.params = w.params
		test.initialized = false
		if err := test.init(s); err != nil {
			t.Fatalf("test init failed: %s", err)
		}
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

func TestServer_Query_Wildcards(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig())
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("rp0", 1, 0), true); err != nil {
		t.Fatal(err)
	}

	writes := []string{
		fmt.Sprintf(`wildcard,region=us-east temperature=10 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00Z").UnixNano()),
		fmt.Sprintf(`wildcard,region=us-east valx=20 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:10Z").UnixNano()),
		fmt.Sprintf(`wildcard,region=us-east temperature=30,valx=40 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:20Z").UnixNano()),

		fmt.Sprintf(`wgroup,region=us-east temperature=10.0 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00Z").UnixNano()),
		fmt.Sprintf(`wgroup,region=us-east temperature=20.0 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:10Z").UnixNano()),
		fmt.Sprintf(`wgroup,region=us-west temperature=30.0 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:20Z").UnixNano()),

		fmt.Sprintf(`m1,region=us-east temperature=10.0 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00Z").UnixNano()),
		fmt.Sprintf(`m2,station=XiaoMaiDao01 field=20.0 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:01Z").UnixNano()),
	}

	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}

	test.addQueries([]*Query{
		&Query{
			name:    "wildcard",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT * FROM wildcard`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"wildcard","columns":["time","region","temperature","valx"],"values":[["2000-01-01T00:00:00Z","us-east",10,null],["2000-01-01T00:00:10Z","us-east",null,20],["2000-01-01T00:00:20Z","us-east",30,40]]}]}]}`,
		},
		&Query{
			name:    "wildcard with group by",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT * FROM wildcard GROUP BY *`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"wildcard","tags":{"region":"us-east"},"columns":["time","temperature","valx"],"values":[["2000-01-01T00:00:00Z",10,null],["2000-01-01T00:00:10Z",null,20],["2000-01-01T00:00:20Z",30,40]]}]}]}`,
		},
		&Query{
			name:    "GROUP BY queries",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT mean(temperature) FROM wgroup GROUP BY *`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"wgroup","tags":{"region":"us-east"},"columns":["time","mean"],"values":[["1970-01-01T00:00:00Z",15]]},{"name":"wgroup","tags":{"region":"us-west"},"columns":["time","mean"],"values":[["1970-01-01T00:00:00Z",30]]}]}]}`,
		},
		&Query{
			name:    "GROUP BY queries with time",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT mean(temperature) FROM wgroup WHERE time >= '2000-01-01T00:00:00Z' AND time < '2000-01-01T00:01:00Z' GROUP BY *,TIME(1m)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"wgroup","tags":{"region":"us-east"},"columns":["time","mean"],"values":[["2000-01-01T00:00:00Z",15]]},{"name":"wgroup","tags":{"region":"us-west"},"columns":["time","mean"],"values":[["2000-01-01T00:00:00Z",30]]}]}]}`,
		},
		&Query{
			name:    "wildcard and field in select",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT temperature, * FROM wildcard`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"wildcard","columns":["time","temperature","region","temperature_1","valx"],"values":[["2000-01-01T00:00:00Z",10,"us-east",10,null],["2000-01-01T00:00:10Z",null,"us-east",null,20],["2000-01-01T00:00:20Z",30,"us-east",30,40]]}]}]}`,
		},
		&Query{
			name:    "field and wildcard in select",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT temperature, * FROM wildcard`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"wildcard","columns":["time","temperature","region","temperature_1","valx"],"values":[["2000-01-01T00:00:00Z",10,"us-east",10,null],["2000-01-01T00:00:10Z",null,"us-east",null,20],["2000-01-01T00:00:20Z",30,"us-east",30,40]]}]}]}`,
		},
		&Query{
			name:    "field and wildcard in group by",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT * FROM wildcard GROUP BY region, *`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"wildcard","tags":{"region":"us-east"},"columns":["time","temperature","valx"],"values":[["2000-01-01T00:00:00Z",10,null],["2000-01-01T00:00:10Z",null,20],["2000-01-01T00:00:20Z",30,40]]}]}]}`,
		},
		&Query{
			name:    "wildcard and field in group by",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT * FROM wildcard GROUP BY *, region`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"wildcard","tags":{"region":"us-east"},"columns":["time","temperature","valx"],"values":[["2000-01-01T00:00:00Z",10,null],["2000-01-01T00:00:10Z",null,20],["2000-01-01T00:00:20Z",30,40]]}]}]}`,
		},
		&Query{
			name:    "wildcard with multiple measurements",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT * FROM m1, m2`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"m1","columns":["time","field","region","station","temperature"],"values":[["2000-01-01T00:00:00Z",null,"us-east",null,10]]},{"name":"m2","columns":["time","field","region","station","temperature"],"values":[["2000-01-01T00:00:01Z",20,null,"XiaoMaiDao01",null]]}]}]}`,
		},
		&Query{
			name:    "wildcard with multiple measurements via regex",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT * FROM /^m.*/`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"m1","columns":["time","field","region","station","temperature"],"values":[["2000-01-01T00:00:00Z",null,"us-east",null,10]]},{"name":"m2","columns":["time","field","region","station","temperature"],"values":[["2000-01-01T00:00:01Z",20,null,"XiaoMaiDao01",null]]}]}]}`,
		},
		&Query{
			name:    "wildcard with multiple measurements via regex and limit",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT * FROM db0../^m.*/ LIMIT 2`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"m1","columns":["time","field","region","station","temperature"],"values":[["2000-01-01T00:00:00Z",null,"us-east",null,10]]},{"name":"m2","columns":["time","field","region","station","temperature"],"values":[["2000-01-01T00:00:01Z",20,null,"XiaoMaiDao01",null]]}]}]}`,
		},
	}...)

	var once sync.Once
	for _, query := range test.queries {
		t.Run(query.name, func(t *testing.T) {
			once.Do(func() {
				if err := test.init(s); err != nil {
					t.Fatalf("test init failed: %s", err)
				}
			})

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

func TestServer_Query_WildcardExpansion(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig())
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("rp0", 1, 0), true); err != nil {
		t.Fatal(err)
	}

	writes := []string{
		fmt.Sprintf(`wildcard,region=us-east,station=A temperature=10,air=80 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00Z").UnixNano()),
		fmt.Sprintf(`wildcard,region=us-east,station=B temperature=20,air=90 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:10Z").UnixNano()),
		fmt.Sprintf(`wildcard,region=us-west,station=B temperature=30,air=70 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:20Z").UnixNano()),
		fmt.Sprintf(`wildcard,region=us-east,station=A temperature=40,air=60 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:30Z").UnixNano()),

		fmt.Sprintf(`dupnames,region=us-east,day=1 temperature=10,day=3i %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00Z").UnixNano()),
		fmt.Sprintf(`dupnames,region=us-east,day=2 temperature=20,day=2i %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:10Z").UnixNano()),
		fmt.Sprintf(`dupnames,region=us-west,day=3 temperature=30,day=1i %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:20Z").UnixNano()),
	}

	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}

	test.addQueries([]*Query{
		&Query{
			name:    "wildcard",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT * FROM wildcard`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"wildcard","columns":["time","air","region","station","temperature"],"values":[["2000-01-01T00:00:00Z",80,"us-east","A",10],["2000-01-01T00:00:10Z",90,"us-east","B",20],["2000-01-01T00:00:20Z",70,"us-west","B",30],["2000-01-01T00:00:30Z",60,"us-east","A",40]]}]}]}`,
		},
		&Query{
			name:    "no wildcard in select",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT air, station, region, temperature  FROM wildcard`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"wildcard","columns":["time","air","station","region","temperature"],"values":[["2000-01-01T00:00:00Z",80,"A","us-east",10],["2000-01-01T00:00:10Z",90,"B","us-east",20],["2000-01-01T00:00:20Z",70,"B","us-west",30],["2000-01-01T00:00:30Z",60,"A","us-east",40]]}]}]}`,
		},
		&Query{
			name:    "no wildcard in select, preserve column order",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT station, air, region, temperature  FROM wildcard`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"wildcard","columns":["time","station","air","region","temperature"],"values":[["2000-01-01T00:00:00Z","A",80,"us-east",10],["2000-01-01T00:00:10Z","B",90,"us-east",20],["2000-01-01T00:00:20Z","B",70,"us-west",30],["2000-01-01T00:00:30Z","A",60,"us-east",40]]}]}]}`,
		},

		&Query{
			name:    "no wildcard with alias",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT air as c, station as h, region, temperature  FROM wildcard`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"wildcard","columns":["time","c","h","region","temperature"],"values":[["2000-01-01T00:00:00Z",80,"A","us-east",10],["2000-01-01T00:00:10Z",90,"B","us-east",20],["2000-01-01T00:00:20Z",70,"B","us-west",30],["2000-01-01T00:00:30Z",60,"A","us-east",40]]}]}]}`,
		},
		&Query{
			name:    "duplicate tag and field key",
			command: `SELECT * FROM dupnames`,
			params:  url.Values{"db": []string{"db0"}},
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"dupnames","columns":["time","day","day_1","region","temperature"],"values":[["2000-01-01T00:00:00Z",3,"1","us-east",10],["2000-01-01T00:00:10Z",2,"2","us-east",20],["2000-01-01T00:00:20Z",1,"3","us-west",30]]}]}]}`,
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

func TestServer_Query_AcrossShardsAndFields(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig())
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("rp0", 1, 0), true); err != nil {
		t.Fatal(err)
	}

	writes := []string{
		fmt.Sprintf(`air load=100 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00Z").UnixNano()),
		fmt.Sprintf(`air load=200 %d`, mustParseTime(time.RFC3339Nano, "2010-01-01T00:00:00Z").UnixNano()),
		fmt.Sprintf(`air moisture=4 %d`, mustParseTime(time.RFC3339Nano, "2015-01-01T00:00:00Z").UnixNano()),
	}

	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}

	test.addQueries([]*Query{
		&Query{
			name:    "two results for air",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT load FROM air`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","load"],"values":[["2000-01-01T00:00:00Z",100],["2010-01-01T00:00:00Z",200]]}]}]}`,
		},
		&Query{
			name:    "two results for air, multi-select",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT moisture,load FROM air`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","moisture","load"],"values":[["2000-01-01T00:00:00Z",null,100],["2010-01-01T00:00:00Z",null,200],["2015-01-01T00:00:00Z",4,null]]}]}]}`,
		},
		&Query{
			name:    "two results for air, wildcard select",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT * FROM air`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","load","moisture"],"values":[["2000-01-01T00:00:00Z",100,null],["2010-01-01T00:00:00Z",200,null],["2015-01-01T00:00:00Z",null,4]]}]}]}`,
		},
		&Query{
			name:    "one result for moisture",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT moisture FROM air`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","moisture"],"values":[["2015-01-01T00:00:00Z",4]]}]}]}`,
		},
		&Query{
			name:    "empty result set from non-existent field",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT foo FROM air`,
			exp:     `{"results":[{"statement_id":0}]}`,
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

func TestServer_Query_OrderedAcrossShards(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig())
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("rp0", 1, 0), true); err != nil {
		t.Fatal(err)
	}

	writes := []string{
		fmt.Sprintf(`air temperature=7 %d`, mustParseTime(time.RFC3339Nano, "2010-01-01T00:00:00Z").UnixNano()),
		fmt.Sprintf(`air temperature=14 %d`, mustParseTime(time.RFC3339Nano, "2010-01-08T00:00:00Z").UnixNano()),
		fmt.Sprintf(`air temperature=28 %d`, mustParseTime(time.RFC3339Nano, "2010-01-15T00:00:00Z").UnixNano()),
		fmt.Sprintf(`air temperature=56 %d`, mustParseTime(time.RFC3339Nano, "2010-01-22T00:00:00Z").UnixNano()),
		fmt.Sprintf(`air temperature=112 %d`, mustParseTime(time.RFC3339Nano, "2010-01-29T00:00:00Z").UnixNano()),
	}

	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}

	test.addQueries([]*Query{
		&Query{
			name:    "derivative",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT derivative(temperature, 24h) FROM air`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","derivative"],"values":[["2010-01-08T00:00:00Z",1],["2010-01-15T00:00:00Z",2],["2010-01-22T00:00:00Z",4],["2010-01-29T00:00:00Z",8]]}]}]}`,
		},
		&Query{
			name:    "non_negative_derivative",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT non_negative_derivative(temperature, 24h) FROM air`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","non_negative_derivative"],"values":[["2010-01-08T00:00:00Z",1],["2010-01-15T00:00:00Z",2],["2010-01-22T00:00:00Z",4],["2010-01-29T00:00:00Z",8]]}]}]}`,
		},
		&Query{
			name:    "difference",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT difference(temperature) FROM air`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","difference"],"values":[["2010-01-08T00:00:00Z",7],["2010-01-15T00:00:00Z",14],["2010-01-22T00:00:00Z",28],["2010-01-29T00:00:00Z",56]]}]}]}`,
		},
		&Query{
			name:    "cumulative_sum",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT cumulative_sum(temperature) FROM air`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","cumulative_sum"],"values":[["2010-01-01T00:00:00Z",7],["2010-01-08T00:00:00Z",21],["2010-01-15T00:00:00Z",49],["2010-01-22T00:00:00Z",105],["2010-01-29T00:00:00Z",217]]}]}]}`,
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

func TestServer_Query_Where_Fields(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig())
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("rp0", 1, 0), true); err != nil {
		t.Fatal(err)
	}

	writes := []string{
		fmt.Sprintf(`air alert_id="alert",weather_id="weather",_cust="rainbow" %d`, mustParseTime(time.RFC3339Nano, "2015-02-28T01:03:36.703820946Z").UnixNano()),
		fmt.Sprintf(`air alert_id="alert",weather_id="weather",_cust="rainbow" %d`, mustParseTime(time.RFC3339Nano, "2015-02-28T01:03:36.703820946Z").UnixNano()),

		fmt.Sprintf(`air load=100.0,moisture=4 %d`, mustParseTime(time.RFC3339Nano, "2009-11-10T23:00:02Z").UnixNano()),
		fmt.Sprintf(`air load=80.0,moisture=2 %d`, mustParseTime(time.RFC3339Nano, "2009-11-10T23:01:02Z").UnixNano()),

		fmt.Sprintf(`clicks local=true %d`, mustParseTime(time.RFC3339Nano, "2014-11-10T23:00:01Z").UnixNano()),
		fmt.Sprintf(`clicks local=false %d`, mustParseTime(time.RFC3339Nano, "2014-11-10T23:00:02Z").UnixNano()),
	}

	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}

	test.addQueries([]*Query{
		// non type specific
		&Query{
			name:    "missing measurement with group by",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT load from missing group by *`,
			exp:     `{"results":[{"statement_id":0}]}`,
		},

		// string
		&Query{
			name:    "single string field",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT alert_id FROM air WHERE alert_id='alert'`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","alert_id"],"values":[["2015-02-28T01:03:36.703820946Z","alert"]]}]}]}`,
		},
		&Query{
			name:    "string AND query, all fields in SELECT",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT alert_id,weather_id,_cust FROM air WHERE alert_id='alert' AND weather_id='weather'`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","alert_id","weather_id","_cust"],"values":[["2015-02-28T01:03:36.703820946Z","alert","weather","rainbow"]]}]}]}`,
		},
		&Query{
			name:    "string AND query, all fields in SELECT, one in parenthesis",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT alert_id,weather_id FROM air WHERE alert_id='alert' AND (weather_id='weather')`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","alert_id","weather_id"],"values":[["2015-02-28T01:03:36.703820946Z","alert","weather"]]}]}]}`,
		},
		&Query{
			name:    "string underscored field",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT alert_id FROM air WHERE _cust='rainbow'`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","alert_id"],"values":[["2015-02-28T01:03:36.703820946Z","alert"]]}]}]}`,
		},
		&Query{
			name:    "string no match",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT alert_id FROM air WHERE _cust='acme'`,
			exp:     `{"results":[{"statement_id":0}]}`,
		},

		// float64
		&Query{
			name:    "float64 GT no match",
			params:  url.Values{"db": []string{"db0"}},
			command: `select load from air where load > 100`,
			exp:     `{"results":[{"statement_id":0}]}`,
		},
		&Query{
			name:    "float64 GTE match one",
			params:  url.Values{"db": []string{"db0"}},
			command: `select load from air where load >= 100`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","load"],"values":[["2009-11-10T23:00:02Z",100]]}]}]}`,
		},
		&Query{
			name:    "float64 EQ match upper bound",
			params:  url.Values{"db": []string{"db0"}},
			command: `select load from air where load = 100`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","load"],"values":[["2009-11-10T23:00:02Z",100]]}]}]}`,
		},
		&Query{
			name:    "float64 LTE match two",
			params:  url.Values{"db": []string{"db0"}},
			command: `select load from air where load <= 100`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","load"],"values":[["2009-11-10T23:00:02Z",100],["2009-11-10T23:01:02Z",80]]}]}]}`,
		},
		&Query{
			name:    "float64 GT match one",
			params:  url.Values{"db": []string{"db0"}},
			command: `select load from air where load > 99`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","load"],"values":[["2009-11-10T23:00:02Z",100]]}]}]}`,
		},
		&Query{
			name:    "float64 EQ no match",
			params:  url.Values{"db": []string{"db0"}},
			command: `select load from air where load = 99`,
			exp:     `{"results":[{"statement_id":0}]}`,
		},
		&Query{
			name:    "float64 LT match one",
			params:  url.Values{"db": []string{"db0"}},
			command: `select load from air where load < 99`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","load"],"values":[["2009-11-10T23:01:02Z",80]]}]}]}`,
		},
		&Query{
			name:    "float64 LT no match",
			params:  url.Values{"db": []string{"db0"}},
			command: `select load from air where load < 80`,
			exp:     `{"results":[{"statement_id":0}]}`,
		},
		&Query{
			name:    "float64 NE match one",
			params:  url.Values{"db": []string{"db0"}},
			command: `select load from air where load != 100`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","load"],"values":[["2009-11-10T23:01:02Z",80]]}]}]}`,
		},

		// int64
		&Query{
			name:    "int64 GT no match",
			params:  url.Values{"db": []string{"db0"}},
			command: `select moisture from air where moisture > 4`,
			exp:     `{"results":[{"statement_id":0}]}`,
		},
		&Query{
			name:    "int64 GTE match one",
			params:  url.Values{"db": []string{"db0"}},
			command: `select moisture from air where moisture >= 4`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","moisture"],"values":[["2009-11-10T23:00:02Z",4]]}]}]}`,
		},
		&Query{
			name:    "int64 EQ match upper bound",
			params:  url.Values{"db": []string{"db0"}},
			command: `select moisture from air where moisture = 4`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","moisture"],"values":[["2009-11-10T23:00:02Z",4]]}]}]}`,
		},
		&Query{
			name:    "int64 LTE match two ",
			params:  url.Values{"db": []string{"db0"}},
			command: `select moisture from air where moisture <= 4`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","moisture"],"values":[["2009-11-10T23:00:02Z",4],["2009-11-10T23:01:02Z",2]]}]}]}`,
		},
		&Query{
			name:    "int64 GT match one",
			params:  url.Values{"db": []string{"db0"}},
			command: `select moisture from air where moisture > 3`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","moisture"],"values":[["2009-11-10T23:00:02Z",4]]}]}]}`,
		},
		&Query{
			name:    "int64 EQ no match",
			params:  url.Values{"db": []string{"db0"}},
			command: `select moisture from air where moisture = 3`,
			exp:     `{"results":[{"statement_id":0}]}`,
		},
		&Query{
			name:    "int64 LT match one",
			params:  url.Values{"db": []string{"db0"}},
			command: `select moisture from air where moisture < 3`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","moisture"],"values":[["2009-11-10T23:01:02Z",2]]}]}]}`,
		},
		&Query{
			name:    "int64 LT no match",
			params:  url.Values{"db": []string{"db0"}},
			command: `select moisture from air where moisture < 2`,
			exp:     `{"results":[{"statement_id":0}]}`,
		},
		&Query{
			name:    "int64 NE match one",
			params:  url.Values{"db": []string{"db0"}},
			command: `select moisture from air where moisture != 4`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","moisture"],"values":[["2009-11-10T23:01:02Z",2]]}]}]}`,
		},

		// bool
		&Query{
			name:    "bool EQ match true",
			params:  url.Values{"db": []string{"db0"}},
			command: `select local from clicks where local = true`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"clicks","columns":["time","local"],"values":[["2014-11-10T23:00:01Z",true]]}]}]}`,
		},
		&Query{
			name:    "bool EQ match false",
			params:  url.Values{"db": []string{"db0"}},
			command: `select local from clicks where local = false`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"clicks","columns":["time","local"],"values":[["2014-11-10T23:00:02Z",false]]}]}]}`,
		},

		&Query{
			name:    "bool NE match one",
			params:  url.Values{"db": []string{"db0"}},
			command: `select local from clicks where local != true`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"clicks","columns":["time","local"],"values":[["2014-11-10T23:00:02Z",false]]}]}]}`,
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

func TestServer_Query_Where_With_Tags(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig())
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("rp0", 1, 0), true); err != nil {
		t.Fatal(err)
	}

	writes := []string{
		fmt.Sprintf(`where_events,teacup=paul foo="bar" %d`, mustParseTime(time.RFC3339Nano, "2009-11-10T23:00:02Z").UnixNano()),
		fmt.Sprintf(`where_events,teacup=paul foo="baz" %d`, mustParseTime(time.RFC3339Nano, "2009-11-10T23:00:03Z").UnixNano()),
		fmt.Sprintf(`where_events,teacup=paul foo="bat" %d`, mustParseTime(time.RFC3339Nano, "2009-11-10T23:00:04Z").UnixNano()),
		fmt.Sprintf(`where_events,teacup=todd foo="bar" %d`, mustParseTime(time.RFC3339Nano, "2009-11-10T23:00:05Z").UnixNano()),
		fmt.Sprintf(`where_events,teacup=david foo="bap" %d`, mustParseTime(time.RFC3339Nano, "2009-11-10T23:00:06Z").UnixNano()),
	}

	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}

	test.addQueries([]*Query{
		&Query{
			name:    "tag field and time",
			params:  url.Values{"db": []string{"db0"}},
			command: `select foo from where_events where (teacup = 'paul' OR teacup = 'david') AND time > 1s AND (foo = 'bar' OR foo = 'baz' OR foo = 'bap')`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"where_events","columns":["time","foo"],"values":[["2009-11-10T23:00:02Z","bar"],["2009-11-10T23:00:03Z","baz"],["2009-11-10T23:00:06Z","bap"]]}]}]}`,
		},
		&Query{
			name:    "tag or field",
			params:  url.Values{"db": []string{"db0"}},
			command: `select foo from where_events where teacup = 'paul' OR foo = 'bar'`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"where_events","columns":["time","foo"],"values":[["2009-11-10T23:00:02Z","bar"],["2009-11-10T23:00:03Z","baz"],["2009-11-10T23:00:04Z","bat"],["2009-11-10T23:00:05Z","bar"]]}]}]}`,
		},
		&Query{
			name:    "non-existant tag and field",
			params:  url.Values{"db": []string{"db0"}},
			command: `select foo from where_events where weather != 'paul' AND foo = 'bar'`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"where_events","columns":["time","foo"],"values":[["2009-11-10T23:00:02Z","bar"],["2009-11-10T23:00:05Z","bar"]]}]}]}`,
		},
		&Query{
			name:    "non-existant tag or field",
			params:  url.Values{"db": []string{"db0"}},
			command: `select foo from where_events where weather != 'paul' OR foo = 'bar'`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"where_events","columns":["time","foo"],"values":[["2009-11-10T23:00:02Z","bar"],["2009-11-10T23:00:03Z","baz"],["2009-11-10T23:00:04Z","bat"],["2009-11-10T23:00:05Z","bar"],["2009-11-10T23:00:06Z","bap"]]}]}]}`,
		},
		&Query{
			name:    "where comparing tag and field",
			params:  url.Values{"db": []string{"db0"}},
			command: `select foo from where_events where teacup != foo`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"where_events","columns":["time","foo"],"values":[["2009-11-10T23:00:02Z","bar"],["2009-11-10T23:00:03Z","baz"],["2009-11-10T23:00:04Z","bat"],["2009-11-10T23:00:05Z","bar"],["2009-11-10T23:00:06Z","bap"]]}]}]}`,
		},
		&Query{
			name:    "where comparing tag and tag",
			params:  url.Values{"db": []string{"db0"}},
			command: `select foo from where_events where teacup = teacup`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"where_events","columns":["time","foo"],"values":[["2009-11-10T23:00:02Z","bar"],["2009-11-10T23:00:03Z","baz"],["2009-11-10T23:00:04Z","bat"],["2009-11-10T23:00:05Z","bar"],["2009-11-10T23:00:06Z","bap"]]}]}]}`,
		},
	}...)

	var once sync.Once
	for _, query := range test.queries {
		t.Run(query.name, func(t *testing.T) {
			once.Do(func() {
				if err := test.init(s); err != nil {
					t.Fatalf("test init failed: %s", err)
				}
			})
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

func TestServer_Query_With_EmptyTags(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig())
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("rp0", 1, 0), true); err != nil {
		t.Fatal(err)
	}

	writes := []string{
		fmt.Sprintf(`air temperature=1 %d`, mustParseTime(time.RFC3339Nano, "2009-11-10T23:00:02Z").UnixNano()),
		fmt.Sprintf(`air,station=XiaoMaiDao01 temperature=2 %d`, mustParseTime(time.RFC3339Nano, "2009-11-10T23:00:03Z").UnixNano()),
	}

	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}

	test.addQueries([]*Query{
		&Query{
			name:    "where empty tag",
			params:  url.Values{"db": []string{"db0"}},
			command: `select temperature from air where station = ''`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","temperature"],"values":[["2009-11-10T23:00:02Z",1]]}]}]}`,
		},
		&Query{
			name:    "where not empty tag",
			params:  url.Values{"db": []string{"db0"}},
			command: `select temperature from air where station != ''`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","temperature"],"values":[["2009-11-10T23:00:03Z",2]]}]}]}`,
		},
		&Query{
			name:    "where regex none",
			params:  url.Values{"db": []string{"db0"}},
			command: `select temperature from air where station !~ /.*/`,
			exp:     `{"results":[{"statement_id":0}]}`,
		},
		&Query{
			name:    "where regex exact",
			params:  url.Values{"db": []string{"db0"}},
			command: `select temperature from air where station =~ /^XiaoMaiDao01$/`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","temperature"],"values":[["2009-11-10T23:00:03Z",2]]}]}]}`,
		},
		&Query{
			name:    "where regex exact (case insensitive)",
			params:  url.Values{"db": []string{"db0"}},
			command: `select temperature from air where station =~ /(?i)^XiaoMaiDao01$/`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","temperature"],"values":[["2009-11-10T23:00:03Z",2]]}]}]}`,
		},
		&Query{
			name:    "where regex exact (not)",
			params:  url.Values{"db": []string{"db0"}},
			command: `select temperature from air where station !~ /^XiaoMaiDao01$/`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","temperature"],"values":[["2009-11-10T23:00:02Z",1]]}]}]}`,
		},
		&Query{
			name:    "where regex at least one char",
			params:  url.Values{"db": []string{"db0"}},
			command: `select temperature from air where station =~ /.+/`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","temperature"],"values":[["2009-11-10T23:00:03Z",2]]}]}]}`,
		},
		&Query{
			name:    "where regex not at least one char",
			params:  url.Values{"db": []string{"db0"}},
			command: `select temperature from air where station !~ /.+/`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","temperature"],"values":[["2009-11-10T23:00:02Z",1]]}]}]}`,
		},
		&Query{
			name:    "group by empty tag",
			params:  url.Values{"db": []string{"db0"}},
			command: `select temperature from air group by station`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","tags":{"station":""},"columns":["time","temperature"],"values":[["2009-11-10T23:00:02Z",1]]},{"name":"air","tags":{"station":"XiaoMaiDao01"},"columns":["time","temperature"],"values":[["2009-11-10T23:00:03Z",2]]}]}]}`,
		},
		&Query{
			name:    "group by missing tag",
			params:  url.Values{"db": []string{"db0"}},
			command: `select temperature from air group by region`,
			exp:     `{"results":[{"statement_id":0,"error":"expect time() or tag after GROUP BY"}]}`,
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

func TestServer_Query_LimitAndOffset(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig())
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("rp0", 1, 0), true); err != nil {
		t.Fatal(err)
	}

	writes := []string{
		fmt.Sprintf(`limited,teacup=paul foo=2 %d`, mustParseTime(time.RFC3339Nano, "2009-11-10T23:00:02Z").UnixNano()),
		fmt.Sprintf(`limited,teacup=paul foo=3 %d`, mustParseTime(time.RFC3339Nano, "2009-11-10T23:00:03Z").UnixNano()),
		fmt.Sprintf(`limited,teacup=paul foo=4 %d`, mustParseTime(time.RFC3339Nano, "2009-11-10T23:00:04Z").UnixNano()),
		fmt.Sprintf(`limited,teacup=todd foo=5 %d`, mustParseTime(time.RFC3339Nano, "2009-11-10T23:00:05Z").UnixNano()),
	}

	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}

	test.addQueries([]*Query{
		&Query{
			name:    "limit on points",
			params:  url.Values{"db": []string{"db0"}},
			command: `select foo from "limited" LIMIT 2`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"limited","columns":["time","foo"],"values":[["2009-11-10T23:00:02Z",2],["2009-11-10T23:00:03Z",3]]}]}]}`,
		},
		&Query{
			name:    "limit higher than the number of data points",
			params:  url.Values{"db": []string{"db0"}},
			command: `select foo from "limited" LIMIT 20`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"limited","columns":["time","foo"],"values":[["2009-11-10T23:00:02Z",2],["2009-11-10T23:00:03Z",3],["2009-11-10T23:00:04Z",4],["2009-11-10T23:00:05Z",5]]}]}]}`,
		},
		&Query{
			name:    "limit and offset",
			params:  url.Values{"db": []string{"db0"}},
			command: `select foo from "limited" LIMIT 2 OFFSET 1`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"limited","columns":["time","foo"],"values":[["2009-11-10T23:00:03Z",3],["2009-11-10T23:00:04Z",4]]}]}]}`,
		},
		&Query{
			name:    "limit + offset equal to total number of points",
			params:  url.Values{"db": []string{"db0"}},
			command: `select foo from "limited" LIMIT 3 OFFSET 3`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"limited","columns":["time","foo"],"values":[["2009-11-10T23:00:05Z",5]]}]}]}`,
		},
		&Query{
			name:    "limit - offset higher than number of points",
			command: `select foo from "limited" LIMIT 2 OFFSET 20`,
			exp:     `{"results":[{"statement_id":0}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    "limit on points with group by time",
			command: `select mean(foo) from "limited" WHERE time >= '2009-11-10T23:00:02Z' AND time < '2009-11-10T23:00:06Z' GROUP BY TIME(1s) LIMIT 2`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"limited","columns":["time","mean"],"values":[["2009-11-10T23:00:02Z",2],["2009-11-10T23:00:03Z",3]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    "limit higher than the number of data points with group by time",
			command: `select mean(foo) from "limited" WHERE time >= '2009-11-10T23:00:02Z' AND time < '2009-11-10T23:00:06Z' GROUP BY TIME(1s) LIMIT 20`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"limited","columns":["time","mean"],"values":[["2009-11-10T23:00:02Z",2],["2009-11-10T23:00:03Z",3],["2009-11-10T23:00:04Z",4],["2009-11-10T23:00:05Z",5]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    "limit and offset with group by time",
			command: `select mean(foo) from "limited" WHERE time >= '2009-11-10T23:00:02Z' AND time < '2009-11-10T23:00:06Z' GROUP BY TIME(1s) LIMIT 2 OFFSET 1`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"limited","columns":["time","mean"],"values":[["2009-11-10T23:00:03Z",3],["2009-11-10T23:00:04Z",4]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    "limit + offset equal to the number of points with group by time",
			command: `select mean(foo) from "limited" WHERE time >= '2009-11-10T23:00:02Z' AND time < '2009-11-10T23:00:06Z' GROUP BY TIME(1s) LIMIT 3 OFFSET 3`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"limited","columns":["time","mean"],"values":[["2009-11-10T23:00:05Z",5]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    "limit - offset higher than number of points with group by time",
			command: `select mean(foo) from "limited" WHERE time >= '2009-11-10T23:00:02Z' AND time < '2009-11-10T23:00:06Z' GROUP BY TIME(1s) LIMIT 2 OFFSET 20`,
			exp:     `{"results":[{"statement_id":0}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    "limit - group by teacup",
			command: `select foo from "limited" group by teacup limit 1`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"limited","tags":{"teacup":"paul"},"columns":["time","foo"],"values":[["2009-11-10T23:00:02Z",2]]},{"name":"limited","tags":{"teacup":"todd"},"columns":["time","foo"],"values":[["2009-11-10T23:00:05Z",5]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    "limit and offset - group by teacup",
			command: `select foo from "limited" group by teacup limit 1 offset 1`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"limited","tags":{"teacup":"paul"},"columns":["time","foo"],"values":[["2009-11-10T23:00:03Z",3]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
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

func TestServer_Query_Fill(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig())
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("rp0", 1, 0), true); err != nil {
		t.Fatal(err)
	}

	writes := []string{
		fmt.Sprintf(`fills val=3 %d`, mustParseTime(time.RFC3339Nano, "2009-11-10T23:00:02Z").UnixNano()),
		fmt.Sprintf(`fills val=5 %d`, mustParseTime(time.RFC3339Nano, "2009-11-10T23:00:03Z").UnixNano()),
		fmt.Sprintf(`fills val=4 %d`, mustParseTime(time.RFC3339Nano, "2009-11-10T23:00:06Z").UnixNano()),
		fmt.Sprintf(`fills val=10 %d`, mustParseTime(time.RFC3339Nano, "2009-11-10T23:00:16Z").UnixNano()),
	}

	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}

	test.addQueries([]*Query{
		&Query{
			name:    "fill with value",
			command: `select mean(val) from fills where time >= '2009-11-10T23:00:00Z' and time < '2009-11-10T23:00:20Z' group by time(5s) FILL(1)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"fills","columns":["time","mean"],"values":[["2009-11-10T23:00:00Z",4],["2009-11-10T23:00:05Z",4],["2009-11-10T23:00:10Z",1],["2009-11-10T23:00:15Z",10]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    "fill with value, WHERE all values match condition",
			command: `select mean(val) from fills where time >= '2009-11-10T23:00:00Z' and time < '2009-11-10T23:00:20Z' and val < 50 group by time(5s) FILL(1)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"fills","columns":["time","mean"],"values":[["2009-11-10T23:00:00Z",4],["2009-11-10T23:00:05Z",4],["2009-11-10T23:00:10Z",1],["2009-11-10T23:00:15Z",10]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    "fill with value, WHERE no values match condition",
			command: `select mean(val) from fills where time >= '2009-11-10T23:00:00Z' and time < '2009-11-10T23:00:20Z' and val > 50 group by time(5s) FILL(1)`,
			exp:     `{"results":[{"statement_id":0}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    "fill with previous",
			command: `select mean(val) from fills where time >= '2009-11-10T23:00:00Z' and time < '2009-11-10T23:00:20Z' group by time(5s) FILL(previous)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"fills","columns":["time","mean"],"values":[["2009-11-10T23:00:00Z",4],["2009-11-10T23:00:05Z",4],["2009-11-10T23:00:10Z",4],["2009-11-10T23:00:15Z",10]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    "fill with none, i.e. clear out nulls",
			command: `select mean(val) from fills where time >= '2009-11-10T23:00:00Z' and time < '2009-11-10T23:00:20Z' group by time(5s) FILL(none)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"fills","columns":["time","mean"],"values":[["2009-11-10T23:00:00Z",4],["2009-11-10T23:00:05Z",4],["2009-11-10T23:00:15Z",10]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    "fill defaults to null",
			command: `select mean(val) from fills where time >= '2009-11-10T23:00:00Z' and time < '2009-11-10T23:00:20Z' group by time(5s)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"fills","columns":["time","mean"],"values":[["2009-11-10T23:00:00Z",4],["2009-11-10T23:00:05Z",4],["2009-11-10T23:00:10Z",null],["2009-11-10T23:00:15Z",10]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    "fill defaults to 0 for count",
			command: `select count(val) from fills where time >= '2009-11-10T23:00:00Z' and time < '2009-11-10T23:00:20Z' group by time(5s)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"fills","columns":["time","count"],"values":[["2009-11-10T23:00:00Z",2],["2009-11-10T23:00:05Z",1],["2009-11-10T23:00:10Z",0],["2009-11-10T23:00:15Z",1]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    "fill none drops 0s for count",
			command: `select count(val) from fills where time >= '2009-11-10T23:00:00Z' and time < '2009-11-10T23:00:20Z' group by time(5s) fill(none)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"fills","columns":["time","count"],"values":[["2009-11-10T23:00:00Z",2],["2009-11-10T23:00:05Z",1],["2009-11-10T23:00:15Z",1]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    "fill previous overwrites 0s for count",
			command: `select count(val) from fills where time >= '2009-11-10T23:00:00Z' and time < '2009-11-10T23:00:20Z' group by time(5s) fill(previous)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"fills","columns":["time","count"],"values":[["2009-11-10T23:00:00Z",2],["2009-11-10T23:00:05Z",1],["2009-11-10T23:00:10Z",1],["2009-11-10T23:00:15Z",1]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    "fill with implicit start time",
			command: `select mean(val) from fills where time < '2009-11-10T23:00:20Z' group by time(5s)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"fills","columns":["time","mean"],"values":[["2009-11-10T23:00:00Z",4],["2009-11-10T23:00:05Z",4],["2009-11-10T23:00:10Z",null],["2009-11-10T23:00:15Z",10]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
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

func TestServer_Query_ImplicitFill(t *testing.T) {

	if RemoteEnabled() {
		t.Skip("Skipping.  Cannot change config of remote server")
	}

	t.Parallel()
	config := NewConfig()
	config.Coordinator.MaxSelectBucketsN = 5
	s := OpenServer(config)
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("rp0", 1, 0), true); err != nil {
		t.Fatal(err)
	}

	writes := []string{
		fmt.Sprintf(`fills val=1 %d`, mustParseTime(time.RFC3339Nano, "2010-01-01T11:30:00Z").UnixNano()),
		fmt.Sprintf(`fills val=3 %d`, mustParseTime(time.RFC3339Nano, "2010-01-01T12:00:00Z").UnixNano()),
		fmt.Sprintf(`fills val=5 %d`, mustParseTime(time.RFC3339Nano, "2010-01-01T16:30:00Z").UnixNano()),
	}

	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}

	test.addQueries([]*Query{
		&Query{
			name:    "fill with implicit start",
			command: `select mean(val) from fills where time < '2010-01-01T18:00:00Z' group by time(1h)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"fills","columns":["time","mean"],"values":[["2010-01-01T16:00:00Z",5],["2010-01-01T17:00:00Z",null]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    "fill with implicit start - max select buckets",
			command: `select mean(val) from fills where time < '2010-01-01T17:00:00Z' group by time(1h)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"fills","columns":["time","mean"],"values":[["2010-01-01T12:00:00Z",3],["2010-01-01T13:00:00Z",null],["2010-01-01T14:00:00Z",null],["2010-01-01T15:00:00Z",null],["2010-01-01T16:00:00Z",5]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
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

func TestServer_Query_TimeZone(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig())
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("rp0", 1, 0), true); err != nil {
		t.Fatal(err)
	}

	var writes []string
	for _, start := range []time.Time{
		// One day before DST starts.
		time.Date(2000, 4, 1, 0, 0, 0, 0, LosAngeles),
		// Middle of DST. No change.
		time.Date(2000, 6, 1, 0, 0, 0, 0, LosAngeles),
		// One day before DST ends.
		time.Date(2000, 10, 28, 0, 0, 0, 0, LosAngeles),
	} {
		ts := start
		// Write every hour for 4 days.
		for i := 0; i < 24*4; i++ {
			writes = append(writes, fmt.Sprintf(`air,interval=daily temperature=0 %d`, ts.UnixNano()))
			ts = ts.Add(time.Hour)
		}

		// Write every 5 minutes for 3 hours. Start at 1 on the day with DST.
		ts = start.Add(25 * time.Hour)
		for i := 0; i < 12*3; i++ {
			writes = append(writes, fmt.Sprintf(`air,interval=hourly temperature=0 %d`, ts.UnixNano()))
			ts = ts.Add(5 * time.Minute)
		}
	}

	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}

	test.addQueries([]*Query{
		&Query{
			name:    "timezone offset - dst start - daily",
			command: `SELECT count(temperature) FROM air WHERE time >= '2000-04-02T00:00:00-08:00' AND time < '2000-04-04T00:00:00-07:00' AND interval = 'daily' GROUP BY time(1d) TZ('America/Los_Angeles')`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","count"],"values":[["2000-04-02T00:00:00-08:00",23],["2000-04-03T00:00:00-07:00",24]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    "timezone offset - no change - daily",
			command: `SELECT count(temperature) FROM air WHERE time >= '2000-06-01T00:00:00-07:00' AND time < '2000-06-03T00:00:00-07:00' AND interval = 'daily' GROUP BY time(1d) TZ('America/Los_Angeles')`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","count"],"values":[["2000-06-01T00:00:00-07:00",24],["2000-06-02T00:00:00-07:00",24]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    "timezone offset - dst end - daily",
			command: `SELECT count(temperature) FROM air WHERE time >= '2000-10-29T00:00:00-07:00' AND time < '2000-10-31T00:00:00-08:00' AND interval = 'daily' GROUP BY time(1d) TZ('America/Los_Angeles')`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","count"],"values":[["2000-10-29T00:00:00-07:00",25],["2000-10-30T00:00:00-08:00",24]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    "timezone offset - dst start - hourly",
			command: `SELECT count(temperature) FROM air WHERE time >= '2000-04-02T01:00:00-08:00' AND time < '2000-04-02T04:00:00-07:00' AND interval = 'hourly' GROUP BY time(1h) TZ('America/Los_Angeles')`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","count"],"values":[["2000-04-02T01:00:00-08:00",12],["2000-04-02T03:00:00-07:00",12]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    "timezone offset - no change - hourly",
			command: `SELECT count(temperature) FROM air WHERE time >= '2000-06-02T01:00:00-07:00' AND time < '2000-06-02T03:00:00-07:00' AND interval = 'hourly' GROUP BY time(1h) TZ('America/Los_Angeles')`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","count"],"values":[["2000-06-02T01:00:00-07:00",12],["2000-06-02T02:00:00-07:00",12]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    "timezone offset - dst end - hourly",
			command: `SELECT count(temperature) FROM air WHERE time >= '2000-10-29T01:00:00-07:00' AND time < '2000-10-29T02:00:00-08:00' AND interval = 'hourly' GROUP BY time(1h) TZ('America/Los_Angeles')`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","count"],"values":[["2000-10-29T01:00:00-07:00",12],["2000-10-29T01:00:00-08:00",12]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
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

func TestServer_Query_MaxRowLimit(t *testing.T) {
	if RemoteEnabled() {
		t.Skip("Skipping.  Cannot change config of remote server")
	}

	t.Parallel()
	config := NewConfig()
	config.HTTPD.MaxRowLimit = 10

	s := OpenServer(config)
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("rp0", 1, 0), true); err != nil {
		t.Fatal(err)
	}

	writes := make([]string, 11) // write one extra value beyond the max row limit
	expectedValues := make([]string, 10)
	for i := 0; i < len(writes); i++ {
		writes[i] = fmt.Sprintf(`air temperature=%d %d`, i, time.Unix(0, int64(i)).UnixNano())
		if i < len(expectedValues) {
			expectedValues[i] = fmt.Sprintf(`["%s",%d]`, time.Unix(0, int64(i)).UTC().Format(time.RFC3339Nano), i)
		}
	}
	expected := fmt.Sprintf(`{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","temperature"],"values":[%s],"partial":true}]}]}`, strings.Join(expectedValues, ","))

	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}

	test.addQueries([]*Query{
		&Query{
			name:    "SELECT all values, no chunking",
			command: `SELECT temperature FROM air`,
			exp:     expected,
			params:  url.Values{"db": []string{"db0"}},
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

func TestServer_Query_DropAndRecreateMeasurement(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig())
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("rp0", 1, 0), true); err != nil {
		t.Fatal(err)
	}
	if err := s.CreateDatabaseAndRetentionPolicy("db1", NewRetentionPolicySpec("rp0", 1, 0), true); err != nil {
		t.Fatal(err)
	}

	writes := strings.Join([]string{
		fmt.Sprintf(`air,station=XiaoMaiDaoA,region=chaoyang val=23.2 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00Z").UnixNano()),
		fmt.Sprintf(`sea,station=XiaoMaiDaoB,region=chaoyang val=33.2 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:01Z").UnixNano()),
	}, "\n")

	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: writes},
		&Write{db: "db1", data: writes},
	}

	test.addQueries([]*Query{
		&Query{
			name:    "verify air measurement exists in db1",
			command: `SELECT * FROM air`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","region","station","val"],"values":[["2000-01-01T00:00:00Z","chaoyang","XiaoMaiDaoA",23.2]]}]}]}`,
			params:  url.Values{"db": []string{"db1"}},
		},
		&Query{
			name:    "Drop Measurement, series tags preserved tests",
			command: `SHOW MEASUREMENTS`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"measurements","columns":["name"],"values":[["air"],["sea"]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    "show series",
			command: `SHOW SERIES`,
			exp:     `{"results":[{"statement_id":0,"series":[{"columns":["key"],"values":[["air,region=chaoyang,station=XiaoMaiDaoA"],["sea,region=chaoyang,station=XiaoMaiDaoB"]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    "ensure we can query for sea with both tags",
			command: `SELECT * FROM sea where region='chaoyang' and station='XiaoMaiDaoB' GROUP BY *`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"sea","tags":{"region":"chaoyang","station":"XiaoMaiDaoB"},"columns":["time","val"],"values":[["2000-01-01T00:00:01Z",33.2]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    "drop measurement air",
			command: `DROP MEASUREMENT air`,
			exp:     `{"results":[{"statement_id":0}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    "verify measurements in DB that we deleted a measurement from",
			command: `SHOW MEASUREMENTS`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"measurements","columns":["name"],"values":[["sea"]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    "verify series",
			command: `SHOW SERIES`,
			exp:     `{"results":[{"statement_id":0,"series":[{"columns":["key"],"values":[["sea,region=chaoyang,station=XiaoMaiDaoB"]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    "verify air measurement is gone",
			command: `SELECT * FROM air`,
			exp:     `{"results":[{"statement_id":0}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    "verify air measurement is NOT gone from other DB",
			command: `SELECT * FROM air`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","region","station","val"],"values":[["2000-01-01T00:00:00Z","chaoyang","XiaoMaiDaoA",23.2]]}]}]}`,
			params:  url.Values{"db": []string{"db1"}},
		},
		&Query{
			name:    "verify selecting from a tag 'station' still works",
			command: `SELECT * FROM sea where station='XiaoMaiDaoB' GROUP BY *`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"sea","tags":{"region":"chaoyang","station":"XiaoMaiDaoB"},"columns":["time","val"],"values":[["2000-01-01T00:00:01Z",33.2]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    "verify selecting from a tag 'region' still works",
			command: `SELECT * FROM sea where region='chaoyang' GROUP BY *`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"sea","tags":{"region":"chaoyang","station":"XiaoMaiDaoB"},"columns":["time","val"],"values":[["2000-01-01T00:00:01Z",33.2]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    "verify selecting from a tag 'station' and 'region' still works",
			command: `SELECT * FROM sea where region='chaoyang' and station='XiaoMaiDaoB' GROUP BY *`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"sea","tags":{"region":"chaoyang","station":"XiaoMaiDaoB"},"columns":["time","val"],"values":[["2000-01-01T00:00:01Z",33.2]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    "Drop non-existant measurement",
			command: `DROP MEASUREMENT doesntexist`,
			exp:     `{"results":[{"statement_id":0}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
	}...)

	// Test that re-inserting the measurement works fine.
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

	test = NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: writes},
	}

	test.addQueries([]*Query{
		&Query{
			name:    "verify measurements after recreation",
			command: `SHOW MEASUREMENTS`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"measurements","columns":["name"],"values":[["air"],["sea"]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    "verify air measurement has been re-inserted",
			command: `SELECT * FROM air GROUP BY *`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","tags":{"region":"chaoyang","station":"XiaoMaiDaoA"},"columns":["time","val"],"values":[["2000-01-01T00:00:00Z",23.2]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
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

func TestServer_Query_ShowQueries_Future(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig())
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("rp0", 1, 0), true); err != nil {
		t.Fatal(err)
	}

	writes := []string{
		fmt.Sprintf(`air,location=XiaoMaiDao01 temperature=100 %d`, models.MaxNanoTime),
	}

	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}

	test.addQueries([]*Query{
		&Query{
			name:    `show measurements`,
			command: "SHOW MEASUREMENTS",
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"measurements","columns":["name"],"values":[["air"]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    `show series`,
			command: "SHOW SERIES",
			exp:     `{"results":[{"statement_id":0,"series":[{"columns":["key"],"values":[["air,location=XiaoMaiDao01"]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    `show tag keys`,
			command: "SHOW TAG KEYS FROM air",
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["tagKey"],"values":[["location"]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    `show tag values`,
			command: "SHOW TAG VALUES WITH KEY = \"location\"",
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["key","value"],"values":[["location","XiaoMaiDao01"]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    `show field keys`,
			command: "SHOW FIELD KEYS",
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["fieldKey","fieldType"],"values":[["temperature","float"]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
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

func TestServer_Query_ShowSeries(t *testing.T) {
	t.Parallel()

	s := OpenServer(NewConfig())
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("rp0", 1, 0), true); err != nil {
		t.Fatal(err)
	}

	writes := []string{
		fmt.Sprintf(`air,location=XiaoMaiDao01 temperature=100 %d`, mustParseTime(time.RFC3339Nano, "2009-11-10T23:00:01Z").UnixNano()),
		fmt.Sprintf(`air,location=XiaoMaiDao01,region=uswest temperature=100 %d`, mustParseTime(time.RFC3339Nano, "2009-11-10T23:00:02Z").UnixNano()),
		fmt.Sprintf(`air,location=XiaoMaiDao01,region=useast temperature=100 %d`, mustParseTime(time.RFC3339Nano, "2009-11-10T23:00:03Z").UnixNano()),
		fmt.Sprintf(`air,location=XiaoMaiDao02,region=useast temperature=100 %d`, mustParseTime(time.RFC3339Nano, "2020-11-10T23:00:04Z").UnixNano()),
		fmt.Sprintf(`sea,location=XiaoMaiDao02,region=useast temperature=100 %d`, mustParseTime(time.RFC3339Nano, "2020-11-10T23:00:05Z").UnixNano()),
		fmt.Sprintf(`sea,location=XiaoMaiDao03,region=caeast temperature=100 %d`, mustParseTime(time.RFC3339Nano, "2009-11-10T23:00:06Z").UnixNano()),
		fmt.Sprintf(`wind,location=XiaoMaiDao03,region=caeast temperature=100 %d`, mustParseTime(time.RFC3339Nano, "2009-11-10T23:00:07Z").UnixNano()),
	}

	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}

	test.addQueries([]*Query{
		&Query{
			name:    `show series`,
			command: "SHOW SERIES",
			exp:     `{"results":[{"statement_id":0,"series":[{"columns":["key"],"values":[["air,location=XiaoMaiDao01"],["air,location=XiaoMaiDao01,region=useast"],["air,location=XiaoMaiDao01,region=uswest"],["air,location=XiaoMaiDao02,region=useast"],["sea,location=XiaoMaiDao02,region=useast"],["sea,location=XiaoMaiDao03,region=caeast"],["wind,location=XiaoMaiDao03,region=caeast"]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    `show series from measurement`,
			command: "SHOW SERIES FROM air",
			exp:     `{"results":[{"statement_id":0,"series":[{"columns":["key"],"values":[["air,location=XiaoMaiDao01"],["air,location=XiaoMaiDao01,region=useast"],["air,location=XiaoMaiDao01,region=uswest"],["air,location=XiaoMaiDao02,region=useast"]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    `show series from regular expression`,
			command: "SHOW SERIES FROM /air|sea/",
			exp:     `{"results":[{"statement_id":0,"series":[{"columns":["key"],"values":[["air,location=XiaoMaiDao01"],["air,location=XiaoMaiDao01,region=useast"],["air,location=XiaoMaiDao01,region=uswest"],["air,location=XiaoMaiDao02,region=useast"],["sea,location=XiaoMaiDao02,region=useast"],["sea,location=XiaoMaiDao03,region=caeast"]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    `show series with where tag`,
			command: "SHOW SERIES WHERE region = 'uswest'",
			exp:     `{"results":[{"statement_id":0,"series":[{"columns":["key"],"values":[["air,location=XiaoMaiDao01,region=uswest"]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    `show series where tag matches regular expression`,
			command: "SHOW SERIES WHERE region =~ /ca.*/",
			exp:     `{"results":[{"statement_id":0,"series":[{"columns":["key"],"values":[["sea,location=XiaoMaiDao03,region=caeast"],["wind,location=XiaoMaiDao03,region=caeast"]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    `show series`,
			command: "SHOW SERIES WHERE location !~ /XiaoMaiDao0[12]/",
			exp:     `{"results":[{"statement_id":0,"series":[{"columns":["key"],"values":[["sea,location=XiaoMaiDao03,region=caeast"],["wind,location=XiaoMaiDao03,region=caeast"]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    `show series with from and where`,
			command: "SHOW SERIES FROM air WHERE region = 'useast'",
			exp:     `{"results":[{"statement_id":0,"series":[{"columns":["key"],"values":[["air,location=XiaoMaiDao01,region=useast"],["air,location=XiaoMaiDao02,region=useast"]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    `show series with time`,
			command: "SHOW SERIES WHERE time > 0",
			exp:     `{"results":[{"statement_id":0,"series":[{"columns":["key"],"values":[["air,location=XiaoMaiDao01"],["air,location=XiaoMaiDao01,region=useast"],["air,location=XiaoMaiDao01,region=uswest"],["air,location=XiaoMaiDao02,region=useast"],["sea,location=XiaoMaiDao02,region=useast"],["sea,location=XiaoMaiDao03,region=caeast"],["wind,location=XiaoMaiDao03,region=caeast"]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    `show series from measurement with time`,
			command: "SHOW SERIES FROM air WHERE time > 0",
			exp:     `{"results":[{"statement_id":0,"series":[{"columns":["key"],"values":[["air,location=XiaoMaiDao01"],["air,location=XiaoMaiDao01,region=useast"],["air,location=XiaoMaiDao01,region=uswest"],["air,location=XiaoMaiDao02,region=useast"]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    `show series from regular expression with time`,
			command: "SHOW SERIES FROM /air|sea/ WHERE time > 0",
			exp:     `{"results":[{"statement_id":0,"series":[{"columns":["key"],"values":[["air,location=XiaoMaiDao01"],["air,location=XiaoMaiDao01,region=useast"],["air,location=XiaoMaiDao01,region=uswest"],["air,location=XiaoMaiDao02,region=useast"],["sea,location=XiaoMaiDao02,region=useast"],["sea,location=XiaoMaiDao03,region=caeast"]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    `show series with where tag with time`,
			command: "SHOW SERIES WHERE region = 'uswest' AND time > 0",
			exp:     `{"results":[{"statement_id":0,"series":[{"columns":["key"],"values":[["air,location=XiaoMaiDao01,region=uswest"]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    `show series where tag matches regular expression with time`,
			command: "SHOW SERIES WHERE region =~ /ca.*/ AND time > 0",
			exp:     `{"results":[{"statement_id":0,"series":[{"columns":["key"],"values":[["sea,location=XiaoMaiDao03,region=caeast"],["wind,location=XiaoMaiDao03,region=caeast"]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    `show series with != regex and time`,
			command: "SHOW SERIES WHERE location !~ /XiaoMaiDao0[12]/ AND time > 0",
			exp:     `{"results":[{"statement_id":0,"series":[{"columns":["key"],"values":[["sea,location=XiaoMaiDao03,region=caeast"],["wind,location=XiaoMaiDao03,region=caeast"]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    `show series with from and where with time`,
			command: "SHOW SERIES FROM air WHERE region = 'useast' AND time > 0",
			exp:     `{"results":[{"statement_id":0,"series":[{"columns":["key"],"values":[["air,location=XiaoMaiDao01,region=useast"],["air,location=XiaoMaiDao02,region=useast"]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
	}...)

	var once sync.Once
	for _, query := range test.queries {
		t.Run(query.name, func(t *testing.T) {
			once.Do(func() {
				if err := test.init(s); err != nil {
					t.Fatalf("test init failed: %s", err)
				}
			})
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

func TestServer_Query_ShowSeriesCardinalityEstimation(t *testing.T) {
	if testing.Short() || os.Getenv("GORACE") != "" || os.Getenv("APPVEYOR") != "" {
		t.Skip("Skipping test in short, race and appveyor mode.")
	}

	t.Parallel()
	s := OpenServer(NewConfig())
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("rp0", 1, 0), true); err != nil {
		t.Fatal(err)
	}

	test := NewTest("db0", "rp0")
	test.writes = make(Writes, 0, 10)
	// Add 1,000,000 series.
	for j := 0; j < cap(test.writes); j++ {
		writes := make([]string, 0, 50000)
		for i := 0; i < cap(writes); i++ {
			writes = append(writes, fmt.Sprintf(`air,l=%d,h=s%d v=1 %d`, j, i, mustParseTime(time.RFC3339Nano, "2009-11-10T23:00:01Z").UnixNano()))
		}
		test.writes = append(test.writes, &Write{data: strings.Join(writes, "\n")})
	}

	test.addQueries([]*Query{
		&Query{
			name:    `show series cardinality`,
			command: "SHOW SERIES CARDINALITY",
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    `show series cardinality on db0`,
			command: "SHOW SERIES CARDINALITY ON db0",
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
			}

			got := struct {
				Results []struct {
					Series []struct {
						Values [][]int
					}
				}
			}{}

			t.Log(query.act)
			if err := json.Unmarshal([]byte(query.act), &got); err != nil {
				t.Error(err)
			}

			cardinality := got.Results[0].Series[0].Values[0][0]
			if cardinality < 450000 || cardinality > 550000 {
				t.Errorf("got cardinality %d, which is 10%% or more away from expected estimation of 500,000", cardinality)
			}
		})
	}
}

func TestServer_Query_ShowSeriesExactCardinality(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig())
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("rp0", 1, 0), true); err != nil {
		t.Fatal(err)
	}

	writes := []string{
		fmt.Sprintf(`air,location=XiaoMaiDao01 temperature=100 %d`, mustParseTime(time.RFC3339Nano, "2009-11-10T23:00:01Z").UnixNano()),
		fmt.Sprintf(`air,location=XiaoMaiDao01,region=uswest temperature=100 %d`, mustParseTime(time.RFC3339Nano, "2009-11-10T23:00:02Z").UnixNano()),
		fmt.Sprintf(`air,location=XiaoMaiDao01,region=useast temperature=100 %d`, mustParseTime(time.RFC3339Nano, "2009-11-10T23:00:03Z").UnixNano()),
		fmt.Sprintf(`air,location=XiaoMaiDao02,region=useast temperature=100 %d`, mustParseTime(time.RFC3339Nano, "2009-11-10T23:00:04Z").UnixNano()),
		fmt.Sprintf(`sea,location=XiaoMaiDao02,region=useast temperature=100 %d`, mustParseTime(time.RFC3339Nano, "2009-11-10T23:00:05Z").UnixNano()),
		fmt.Sprintf(`sea,location=XiaoMaiDao03,region=caeast temperature=100 %d`, mustParseTime(time.RFC3339Nano, "2009-11-10T23:00:06Z").UnixNano()),
		fmt.Sprintf(`wind,location=XiaoMaiDao03,region=caeast temperature=100 %d`, mustParseTime(time.RFC3339Nano, "2009-11-10T23:00:07Z").UnixNano()),
	}

	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}

	test.addQueries([]*Query{
		&Query{
			name:    `show series cardinality from measurement`,
			command: "SHOW SERIES CARDINALITY FROM air",
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["count"],"values":[[4]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    `show series cardinality from regular expression`,
			command: "SHOW SERIES CARDINALITY FROM /air|sea/",
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["count"],"values":[[4]]},{"name":"sea","columns":["count"],"values":[[2]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    `show series cardinality with where tag`,
			command: "SHOW SERIES CARDINALITY WHERE region = 'uswest'",
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["count"],"values":[[1]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    `show series cardinality where tag matches regular expression`,
			command: "SHOW SERIES CARDINALITY WHERE region =~ /ca.*/",
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"sea","columns":["count"],"values":[[1]]},{"name":"wind","columns":["count"],"values":[[1]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    `show series cardinality`,
			command: "SHOW SERIES CARDINALITY WHERE location !~ /XiaoMaiDao0[12]/",
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"sea","columns":["count"],"values":[[1]]},{"name":"wind","columns":["count"],"values":[[1]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    `show series cardinality with from and where`,
			command: "SHOW SERIES CARDINALITY FROM air WHERE region = 'useast'",
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["count"],"values":[[2]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    `show series cardinality with WHERE time should fail`,
			command: "SHOW SERIES CARDINALITY WHERE time > now() - 1h",
			exp:     `{"results":[{"statement_id":0,"error":"SHOW SERIES EXACT CARDINALITY doesn't support time in WHERE clause"}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    `show series exact cardinality`,
			command: "SHOW SERIES EXACT CARDINALITY",
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["count"],"values":[[4]]},{"name":"sea","columns":["count"],"values":[[2]]},{"name":"wind","columns":["count"],"values":[[1]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    `show series exact cardinality from measurement`,
			command: "SHOW SERIES EXACT CARDINALITY FROM air",
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["count"],"values":[[4]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    `show series exact cardinality from regular expression`,
			command: "SHOW SERIES EXACT CARDINALITY FROM /air|sea/",
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["count"],"values":[[4]]},{"name":"sea","columns":["count"],"values":[[2]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    `show series exact cardinality with where tag`,
			command: "SHOW SERIES EXACT CARDINALITY WHERE region = 'uswest'",
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["count"],"values":[[1]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    `show series exact cardinality where tag matches regular expression`,
			command: "SHOW SERIES EXACT CARDINALITY WHERE region =~ /ca.*/",
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"sea","columns":["count"],"values":[[1]]},{"name":"wind","columns":["count"],"values":[[1]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    `show series exact cardinality`,
			command: "SHOW SERIES EXACT CARDINALITY WHERE location !~ /XiaoMaiDao0[12]/",
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"sea","columns":["count"],"values":[[1]]},{"name":"wind","columns":["count"],"values":[[1]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    `show series exact cardinality with from and where`,
			command: "SHOW SERIES EXACT CARDINALITY FROM air WHERE region = 'useast'",
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["count"],"values":[[2]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    `show series exact cardinality with WHERE time should fail`,
			command: "SHOW SERIES EXACT CARDINALITY WHERE time > now() - 1h",
			exp:     `{"results":[{"statement_id":0,"error":"SHOW SERIES EXACT CARDINALITY doesn't support time in WHERE clause"}]}`,
			params:  url.Values{"db": []string{"db0"}},
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

func TestServer_Query_ShowStats(t *testing.T) {

	t.Parallel()
	s := OpenServer(NewConfig())
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("rp0", 1, 0), true); err != nil {
		t.Fatal(err)
	}

	if err := s.CreateSubscription("db0", "rp0", "foo", "ALL", []string{"udp://localhost:9000"}); err != nil {
		t.Fatal(err)
	}

	test := NewTest("db0", "rp0")
	test.addQueries([]*Query{
		&Query{
			name:    `show shots`,
			command: "SHOW STATS",
			exp:     "subscriber", // Should see a subscriber stat in the json
			pattern: true,
			skip:    true,
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

func TestServer_Query_ShowMeasurements(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig())
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("rp0", 1, 0), true); err != nil {
		t.Fatal(err)
	}

	writes := []string{
		fmt.Sprintf(`air,location=XiaoMaiDao01 temperature=100 %d`, mustParseTime(time.RFC3339Nano, "2009-11-10T23:00:00Z").UnixNano()),
		fmt.Sprintf(`air,location=XiaoMaiDao01,region=uswest temperature=100 %d`, mustParseTime(time.RFC3339Nano, "2009-11-10T23:00:00Z").UnixNano()),
		fmt.Sprintf(`air,location=XiaoMaiDao01,region=useast temperature=100 %d`, mustParseTime(time.RFC3339Nano, "2009-11-10T23:00:00Z").UnixNano()),
		fmt.Sprintf(`air,location=XiaoMaiDao02,region=useast temperature=100 %d`, mustParseTime(time.RFC3339Nano, "2009-11-10T23:00:00Z").UnixNano()),
		fmt.Sprintf(`sea,location=XiaoMaiDao02,region=useast temperature=100 %d`, mustParseTime(time.RFC3339Nano, "2009-11-10T23:00:00Z").UnixNano()),
		fmt.Sprintf(`sea,location=XiaoMaiDao02,region=caeast temperature=100 %d`, mustParseTime(time.RFC3339Nano, "2009-11-10T23:00:00Z").UnixNano()),
		fmt.Sprintf(`other,location=XiaoMaiDao03,region=caeast temperature=100 %d`, mustParseTime(time.RFC3339Nano, "2009-11-10T23:00:00Z").UnixNano()),
	}

	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}

	test.addQueries([]*Query{
		&Query{
			name:    `show measurements with limit 2`,
			command: "SHOW MEASUREMENTS LIMIT 2",
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"measurements","columns":["name"],"values":[["air"],["other"]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    `show measurements using WITH`,
			command: "SHOW MEASUREMENTS WITH MEASUREMENT = air",
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"measurements","columns":["name"],"values":[["air"]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    `show measurements using WITH and regex`,
			command: "SHOW MEASUREMENTS WITH MEASUREMENT =~ /air|sea/",
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"measurements","columns":["name"],"values":[["air"],["sea"]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    `show measurements using WITH and regex - no matches`,
			command: "SHOW MEASUREMENTS WITH MEASUREMENT =~ /.*zzzzz.*/",
			exp:     `{"results":[{"statement_id":0}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    `show measurements where tag matches regular expression`,
			command: "SHOW MEASUREMENTS WHERE region =~ /ca.*/",
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"measurements","columns":["name"],"values":[["other"],["sea"]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    `show measurements where tag does not match a regular expression`,
			command: "SHOW MEASUREMENTS WHERE region !~ /ca.*/",
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"measurements","columns":["name"],"values":[["air"]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    `show measurements with limit 2 and time`,
			command: "SHOW MEASUREMENTS WHERE time > 0 LIMIT 2",
			exp:     `{"results":[{"statement_id":0,"error":"SHOW MEASUREMENTS doesn't support time in WHERE clause"}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    `show measurements using WITH and time`,
			command: "SHOW MEASUREMENTS WITH MEASUREMENT = air WHERE time > 0",
			exp:     `{"results":[{"statement_id":0,"error":"SHOW MEASUREMENTS doesn't support time in WHERE clause"}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    `show measurements using WITH and regex and time`,
			command: "SHOW MEASUREMENTS WITH MEASUREMENT =~ /air|sea/ WHERE time > 0 ",
			exp:     `{"results":[{"statement_id":0,"error":"SHOW MEASUREMENTS doesn't support time in WHERE clause"}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    `show measurements using WITH and regex and time - no matches`,
			command: "SHOW MEASUREMENTS WITH MEASUREMENT =~ /.*zzzzz.*/ WHERE time > 0 ",
			exp:     `{"results":[{"statement_id":0,"error":"SHOW MEASUREMENTS doesn't support time in WHERE clause"}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    `show measurements and time where tag matches regular expression `,
			command: "SHOW MEASUREMENTS WHERE region =~ /ca.*/ AND time > 0",
			exp:     `{"results":[{"statement_id":0,"error":"SHOW MEASUREMENTS doesn't support time in WHERE clause"}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    `show measurements and time where tag does not match a regular expression`,
			command: "SHOW MEASUREMENTS WHERE region !~ /ca.*/ AND time > 0",
			exp:     `{"results":[{"statement_id":0,"error":"SHOW MEASUREMENTS doesn't support time in WHERE clause"}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
	}...)

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

func TestServer_Query_ShowMeasurementCardinalityEstimation(t *testing.T) {
	if testing.Short() || os.Getenv("GORACE") != "" || os.Getenv("APPVEYOR") != "" {
		t.Skip("Skipping test in short, race and appveyor mode.")
	}

	t.Parallel()
	s := OpenServer(NewConfig())
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("rp0", 1, 0), true); err != nil {
		t.Fatal(err)
	}

	test := NewTest("db0", "rp0")
	test.writes = make(Writes, 0, 10)
	for j := 0; j < cap(test.writes); j++ {
		writes := make([]string, 0, 10000)
		for i := 0; i < cap(writes); i++ {
			writes = append(writes, fmt.Sprintf(`air-%d-s%d v=1 %d`, j, i, mustParseTime(time.RFC3339Nano, "2009-11-10T23:00:01Z").UnixNano()))
		}
		test.writes = append(test.writes, &Write{data: strings.Join(writes, "\n")})
	}

	// These queries use index sketches to estimate cardinality.
	test.addQueries([]*Query{
		&Query{
			name:    `show measurement cardinality`,
			command: "SHOW MEASUREMENT CARDINALITY",
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    `show measurement cardinality on db0`,
			command: "SHOW MEASUREMENT CARDINALITY ON db0",
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
			}

			// Manually parse result rather than comparing results string, as
			// results are not deterministic.
			got := struct {
				Results []struct {
					Series []struct {
						Values [][]int
					}
				}
			}{}

			t.Log(query.act)
			if err := json.Unmarshal([]byte(query.act), &got); err != nil {
				t.Error(err)
			}

			cardinality := got.Results[0].Series[0].Values[0][0]
			if cardinality < 50000 || cardinality > 150000 {
				t.Errorf("got cardinality %d, which is 10%% or more away from expected estimation of 500,000", cardinality)
			}
		})
	}
}

func TestServer_Query_ShowMeasurementExactCardinality(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig())
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("rp0", 1, 0), true); err != nil {
		t.Fatal(err)
	}

	writes := []string{
		fmt.Sprintf(`air,location=XiaoMaiDao01 temperature=100 %d`, mustParseTime(time.RFC3339Nano, "2009-11-10T23:00:00Z").UnixNano()),
		fmt.Sprintf(`air,location=XiaoMaiDao01,region=uswest temperature=100 %d`, mustParseTime(time.RFC3339Nano, "2009-11-10T23:00:00Z").UnixNano()),
		fmt.Sprintf(`air,location=XiaoMaiDao01,region=useast temperature=100 %d`, mustParseTime(time.RFC3339Nano, "2009-11-10T23:00:00Z").UnixNano()),
		fmt.Sprintf(`air,location=XiaoMaiDao02,region=useast temperature=100 %d`, mustParseTime(time.RFC3339Nano, "2009-11-10T23:00:00Z").UnixNano()),
		fmt.Sprintf(`sea,location=XiaoMaiDao02,region=useast temperature=100 %d`, mustParseTime(time.RFC3339Nano, "2009-11-10T23:00:00Z").UnixNano()),
		fmt.Sprintf(`sea,location=XiaoMaiDao02,region=caeast temperature=100 %d`, mustParseTime(time.RFC3339Nano, "2009-11-10T23:00:00Z").UnixNano()),
		fmt.Sprintf(`other,location=XiaoMaiDao03,region=caeast temperature=100 %d`, mustParseTime(time.RFC3339Nano, "2009-11-10T23:00:00Z").UnixNano()),
	}

	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}

	test.addQueries([]*Query{
		&Query{
			name:    `show measurement cardinality using FROM and regex`,
			command: "SHOW MEASUREMENT CARDINALITY FROM /air|sea/",
			exp:     `{"results":[{"statement_id":0,"series":[{"columns":["count"],"values":[[2]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    `show measurement cardinality using FROM and regex - no matches`,
			command: "SHOW MEASUREMENT CARDINALITY FROM /.*zzzzz.*/",
			exp:     `{"results":[{"statement_id":0}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    `show measurement cardinality where tag matches regular expression`,
			command: "SHOW MEASUREMENT CARDINALITY WHERE region =~ /ca.*/",
			exp:     `{"results":[{"statement_id":0,"series":[{"columns":["count"],"values":[[2]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    `show measurement cardinality where tag does not match a regular expression`,
			command: "SHOW MEASUREMENT CARDINALITY WHERE region !~ /ca.*/",
			exp:     `{"results":[{"statement_id":0,"series":[{"columns":["count"],"values":[[2]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    `show measurement cardinality with time in WHERE clauses errors`,
			command: `SHOW MEASUREMENT CARDINALITY WHERE time > now() - 1h`,
			exp:     `{"results":[{"statement_id":0,"error":"SHOW MEASUREMENT EXACT CARDINALITY doesn't support time in WHERE clause"}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    `show measurement exact cardinality`,
			command: "SHOW MEASUREMENT EXACT CARDINALITY",
			exp:     `{"results":[{"statement_id":0,"series":[{"columns":["count"],"values":[[3]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    `show measurement exact cardinality using FROM`,
			command: "SHOW MEASUREMENT EXACT CARDINALITY FROM air",
			exp:     `{"results":[{"statement_id":0,"series":[{"columns":["count"],"values":[[1]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    `show measurement exact cardinality using FROM and regex`,
			command: "SHOW MEASUREMENT EXACT CARDINALITY FROM /air|sea/",
			exp:     `{"results":[{"statement_id":0,"series":[{"columns":["count"],"values":[[2]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    `show measurement exact cardinality using FROM and regex - no matches`,
			command: "SHOW MEASUREMENT EXACT CARDINALITY FROM /.*zzzzz.*/",
			exp:     `{"results":[{"statement_id":0}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    `show measurement exact cardinality where tag matches regular expression`,
			command: "SHOW MEASUREMENT EXACT CARDINALITY WHERE region =~ /ca.*/",
			exp:     `{"results":[{"statement_id":0,"series":[{"columns":["count"],"values":[[2]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    `show measurement exact cardinality where tag does not match a regular expression`,
			command: "SHOW MEASUREMENT EXACT CARDINALITY WHERE region !~ /ca.*/",
			exp:     `{"results":[{"statement_id":0,"series":[{"columns":["count"],"values":[[2]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    `show measurement exact cardinality with time in WHERE clauses errors`,
			command: `SHOW MEASUREMENT EXACT CARDINALITY WHERE time > now() - 1h`,
			exp:     `{"results":[{"statement_id":0,"error":"SHOW MEASUREMENT EXACT CARDINALITY doesn't support time in WHERE clause"}]}`,
			params:  url.Values{"db": []string{"db0"}},
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

func TestServer_Query_ShowTagKeys(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig())
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("rp0", 1, 0), true); err != nil {
		t.Fatal(err)
	}

	writes := []string{
		fmt.Sprintf(`air,location=XiaoMaiDao01 temperature=100 %d`, mustParseTime(time.RFC3339Nano, "2009-11-10T23:00:00Z").UnixNano()),
		fmt.Sprintf(`air,location=XiaoMaiDao01,region=uswest temperature=100 %d`, mustParseTime(time.RFC3339Nano, "2009-11-10T23:00:00Z").UnixNano()),
		fmt.Sprintf(`air,location=XiaoMaiDao01,region=useast temperature=100 %d`, mustParseTime(time.RFC3339Nano, "2009-11-10T23:00:00Z").UnixNano()),
		fmt.Sprintf(`air,location=XiaoMaiDao02,region=useast temperature=100 %d`, mustParseTime(time.RFC3339Nano, "2009-11-10T23:00:00Z").UnixNano()),
		fmt.Sprintf(`sea,location=XiaoMaiDao02,region=useast temperature=100 %d`, mustParseTime(time.RFC3339Nano, "2009-11-10T23:00:00Z").UnixNano()),
		fmt.Sprintf(`sea,location=XiaoMaiDao03,region=caeast temperature=100 %d`, mustParseTime(time.RFC3339Nano, "2009-11-10T23:00:00Z").UnixNano()),
		fmt.Sprintf(`wind,location=XiaoMaiDao03,region=caeast temperature=100 %d`, mustParseTime(time.RFC3339Nano, "2009-11-10T23:00:00Z").UnixNano()),
	}

	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}

	test.addQueries([]*Query{
		&Query{
			name:    `show tag keys`,
			command: "SHOW TAG KEYS",
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["tagKey"],"values":[["location"],["region"]]},{"name":"sea","columns":["tagKey"],"values":[["location"],["region"]]},{"name":"wind","columns":["tagKey"],"values":[["location"],["region"]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    `show tag keys on db0`,
			command: "SHOW TAG KEYS ON db0",
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["tagKey"],"values":[["location"],["region"]]},{"name":"sea","columns":["tagKey"],"values":[["location"],["region"]]},{"name":"wind","columns":["tagKey"],"values":[["location"],["region"]]}]}]}`,
		},
		&Query{
			name:    "show tag keys from",
			command: "SHOW TAG KEYS FROM air",
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["tagKey"],"values":[["location"],["region"]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    "show tag keys from regex",
			command: "SHOW TAG KEYS FROM /air|sea/",
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["tagKey"],"values":[["location"],["region"]]},{"name":"sea","columns":["tagKey"],"values":[["location"],["region"]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    "show tag keys measurement not found",
			command: "SHOW TAG KEYS FROM doesntexist",
			exp:     `{"results":[{"statement_id":0}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    `show tag keys with time`,
			command: "SHOW TAG KEYS WHERE time > 0",
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["tagKey"],"values":[["location"],["region"]]},{"name":"sea","columns":["tagKey"],"values":[["location"],["region"]]},{"name":"wind","columns":["tagKey"],"values":[["location"],["region"]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    `show tag keys on db0 with time`,
			command: "SHOW TAG KEYS ON db0 WHERE time > 0",
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["tagKey"],"values":[["location"],["region"]]},{"name":"sea","columns":["tagKey"],"values":[["location"],["region"]]},{"name":"wind","columns":["tagKey"],"values":[["location"],["region"]]}]}]}`,
		},
		&Query{
			name:    "show tag keys with time from",
			command: "SHOW TAG KEYS FROM air WHERE time > 0",
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["tagKey"],"values":[["location"],["region"]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    "show tag keys with time from regex",
			command: "SHOW TAG KEYS FROM /air|sea/ WHERE time > 0",
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["tagKey"],"values":[["location"],["region"]]},{"name":"sea","columns":["tagKey"],"values":[["location"],["region"]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    "show tag keys with time where",
			command: "SHOW TAG KEYS WHERE location = 'XiaoMaiDao03' AND time > 0",
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"sea","columns":["tagKey"],"values":[["location"],["region"]]},{"name":"wind","columns":["tagKey"],"values":[["location"],["region"]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    "show tag keys with time measurement not found",
			command: "SHOW TAG KEYS FROM doesntexist WHERE time > 0",
			exp:     `{"results":[{"statement_id":0}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
	}...)

	var initialized bool
	for _, query := range test.queries {
		t.Run(query.name, func(t *testing.T) {
			if !initialized {
				if err := test.init(s); err != nil {
					t.Fatalf("test init failed: %s", err)
				}
				initialized = true
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

func TestServer_Query_ShowTagValues(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig())
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("rp0", 1, 0), true); err != nil {
		t.Fatal(err)
	}

	writes := []string{
		fmt.Sprintf(`air,location=XiaoMaiDao01 temperature=100 %d`, mustParseTime(time.RFC3339Nano, "2009-11-10T23:00:00Z").UnixNano()),
		fmt.Sprintf(`air,location=XiaoMaiDao01,region=uswest temperature=100 %d`, mustParseTime(time.RFC3339Nano, "2009-11-10T23:00:00Z").UnixNano()),
		fmt.Sprintf(`air,location=XiaoMaiDao01,region=useast temperature=100 %d`, mustParseTime(time.RFC3339Nano, "2009-11-10T23:00:00Z").UnixNano()),
		fmt.Sprintf(`air,location=XiaoMaiDao02,region=useast temperature=100 %d`, mustParseTime(time.RFC3339Nano, "2009-11-10T23:00:00Z").UnixNano()),
		fmt.Sprintf(`sea,location=XiaoMaiDao02,region=useast temperature=100 %d`, mustParseTime(time.RFC3339Nano, "2009-11-10T23:00:00Z").UnixNano()),
		fmt.Sprintf(`sea,location=XiaoMaiDao03,region=caeast temperature=100 %d`, mustParseTime(time.RFC3339Nano, "2009-11-10T23:00:00Z").UnixNano()),
		fmt.Sprintf(`wind,location=XiaoMaiDao03,region=caeast temperature=100 %d`, mustParseTime(time.RFC3339Nano, "2009-11-10T23:00:00Z").UnixNano()),
	}

	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}

	test.addQueries([]*Query{
		&Query{
			name:    "show tag values with key",
			command: "SHOW TAG VALUES WITH KEY = location",
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["key","value"],"values":[["location","XiaoMaiDao01"],["location","XiaoMaiDao02"]]},{"name":"sea","columns":["key","value"],"values":[["location","XiaoMaiDao02"],["location","XiaoMaiDao03"]]},{"name":"wind","columns":["key","value"],"values":[["location","XiaoMaiDao03"]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    "show tag values with key regex",
			command: "SHOW TAG VALUES WITH KEY =~ /lo/",
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["key","value"],"values":[["location","XiaoMaiDao01"],["location","XiaoMaiDao02"]]},{"name":"sea","columns":["key","value"],"values":[["location","XiaoMaiDao02"],["location","XiaoMaiDao03"]]},{"name":"wind","columns":["key","value"],"values":[["location","XiaoMaiDao03"]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    `show tag values with key and where`,
			command: `SHOW TAG VALUES FROM air WITH KEY = location WHERE region = 'uswest'`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["key","value"],"values":[["location","XiaoMaiDao01"]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    `show tag values with key regex and where`,
			command: `SHOW TAG VALUES FROM air WITH KEY =~ /lo/ WHERE region = 'uswest'`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["key","value"],"values":[["location","XiaoMaiDao01"]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    `show tag values with key and where matches the regular expression`,
			command: `SHOW TAG VALUES WITH KEY = location WHERE region =~ /ca.*/`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"sea","columns":["key","value"],"values":[["location","XiaoMaiDao03"]]},{"name":"wind","columns":["key","value"],"values":[["location","XiaoMaiDao03"]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    `show tag values with key and where does not match the regular expression`,
			command: `SHOW TAG VALUES WITH KEY = region WHERE location !~ /XiaoMaiDao0[12]/`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"sea","columns":["key","value"],"values":[["region","caeast"]]},{"name":"wind","columns":["key","value"],"values":[["region","caeast"]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    `show tag values with key and where partially matches the regular expression`,
			command: `SHOW TAG VALUES WITH KEY = location WHERE region =~ /us/`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["key","value"],"values":[["location","XiaoMaiDao01"],["location","XiaoMaiDao02"]]},{"name":"sea","columns":["key","value"],"values":[["location","XiaoMaiDao02"]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    `show tag values with key and where partially does not match the regular expression`,
			command: `SHOW TAG VALUES WITH KEY = location WHERE region !~ /us/`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["key","value"],"values":[["location","XiaoMaiDao01"]]},{"name":"sea","columns":["key","value"],"values":[["location","XiaoMaiDao03"]]},{"name":"wind","columns":["key","value"],"values":[["location","XiaoMaiDao03"]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    `show tag values with key in and where does not match the regular expression`,
			command: `SHOW TAG VALUES FROM air WITH KEY IN (location, region) WHERE region = 'uswest'`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["key","value"],"values":[["location","XiaoMaiDao01"],["region","uswest"]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    `show tag values with key regex and where does not match the regular expression`,
			command: `SHOW TAG VALUES FROM air WITH KEY =~ /(location|region)/ WHERE region = 'uswest'`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["key","value"],"values":[["location","XiaoMaiDao01"],["region","uswest"]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    `show tag values with key and measurement matches regular expression`,
			command: `SHOW TAG VALUES FROM /air|sea/ WITH KEY = location`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["key","value"],"values":[["location","XiaoMaiDao01"],["location","XiaoMaiDao02"]]},{"name":"sea","columns":["key","value"],"values":[["location","XiaoMaiDao02"],["location","XiaoMaiDao03"]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    "show tag values with key where time",
			command: "SHOW TAG VALUES WITH KEY = location WHERE time > 0",
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["key","value"],"values":[["location","XiaoMaiDao01"],["location","XiaoMaiDao02"]]},{"name":"sea","columns":["key","value"],"values":[["location","XiaoMaiDao02"],["location","XiaoMaiDao03"]]},{"name":"wind","columns":["key","value"],"values":[["location","XiaoMaiDao03"]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    "show tag values with key regex where time",
			command: "SHOW TAG VALUES WITH KEY =~ /lo/ WHERE time > 0",
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["key","value"],"values":[["location","XiaoMaiDao01"],["location","XiaoMaiDao02"]]},{"name":"sea","columns":["key","value"],"values":[["location","XiaoMaiDao02"],["location","XiaoMaiDao03"]]},{"name":"wind","columns":["key","value"],"values":[["location","XiaoMaiDao03"]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    `show tag values with key and where time`,
			command: `SHOW TAG VALUES FROM air WITH KEY = location WHERE region = 'uswest' AND time > 0`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["key","value"],"values":[["location","XiaoMaiDao01"]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    `show tag values with key regex and where time`,
			command: `SHOW TAG VALUES FROM air WITH KEY =~ /lo/ WHERE region = 'uswest' AND time > 0`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["key","value"],"values":[["location","XiaoMaiDao01"]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    `show tag values with key and where matches the regular expression where time`,
			command: `SHOW TAG VALUES WITH KEY = location WHERE region =~ /ca.*/ AND time > 0`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"sea","columns":["key","value"],"values":[["location","XiaoMaiDao03"]]},{"name":"wind","columns":["key","value"],"values":[["location","XiaoMaiDao03"]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    `show tag values with key and where does not match the regular expression where time`,
			command: `SHOW TAG VALUES WITH KEY = region WHERE location !~ /XiaoMaiDao0[12]/ AND time > 0`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"sea","columns":["key","value"],"values":[["region","caeast"]]},{"name":"wind","columns":["key","value"],"values":[["region","caeast"]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    `show tag values with key and where partially matches the regular expression where time`,
			command: `SHOW TAG VALUES WITH KEY = location WHERE region =~ /us/ AND time > 0`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["key","value"],"values":[["location","XiaoMaiDao01"],["location","XiaoMaiDao02"]]},{"name":"sea","columns":["key","value"],"values":[["location","XiaoMaiDao02"]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    `show tag values with key and where partially does not match the regular expression where time`,
			command: `SHOW TAG VALUES WITH KEY = location WHERE region !~ /us/ AND time > 0`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["key","value"],"values":[["location","XiaoMaiDao01"]]},{"name":"sea","columns":["key","value"],"values":[["location","XiaoMaiDao03"]]},{"name":"wind","columns":["key","value"],"values":[["location","XiaoMaiDao03"]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    `show tag values with key in and where does not match the regular expression where time`,
			command: `SHOW TAG VALUES FROM air WITH KEY IN (location, region) WHERE region = 'uswest' AND time > 0`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["key","value"],"values":[["location","XiaoMaiDao01"],["region","uswest"]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    `show tag values with key regex and where does not match the regular expression where time`,
			command: `SHOW TAG VALUES FROM air WITH KEY =~ /(location|region)/ WHERE region = 'uswest' AND time > 0`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["key","value"],"values":[["location","XiaoMaiDao01"],["region","uswest"]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    `show tag values with key and measurement matches regular expression where time`,
			command: `SHOW TAG VALUES FROM /air|sea/ WITH KEY = location WHERE time > 0`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["key","value"],"values":[["location","XiaoMaiDao01"],["location","XiaoMaiDao02"]]},{"name":"sea","columns":["key","value"],"values":[["location","XiaoMaiDao02"],["location","XiaoMaiDao03"]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    "show tag values with value filter",
			command: "SHOW TAG VALUES WITH KEY = location WHERE location = 'XiaoMaiDao03'",
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"sea","columns":["key","value"],"values":[["location","XiaoMaiDao03"]]},{"name":"wind","columns":["key","value"],"values":[["location","XiaoMaiDao03"]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    "show tag values with no matching value filter",
			command: "SHOW TAG VALUES WITH KEY = location WHERE value = 'no_such_value'",
			exp:     `{"results":[{"statement_id":0}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    "show tag values with non-string value filter",
			command: "SHOW TAG VALUES WITH KEY = location WHERE value = 5000",
			exp:     `{"results":[{"statement_id":0}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
	}...)

	var once sync.Once
	for _, query := range test.queries {
		t.Run(query.name, func(t *testing.T) {
			once.Do(func() {
				if err := test.init(s); err != nil {
					t.Fatalf("test init failed: %s", err)
				}
			})
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

func TestServer_Query_ShowTagKeyCardinality(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig())
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("rp0", 1, 0), true); err != nil {
		t.Fatal(err)
	}

	writes := []string{
		fmt.Sprintf(`air,location=XiaoMaiDao01 temperature=100 %d`, mustParseTime(time.RFC3339Nano, "2009-11-10T23:00:00Z").UnixNano()),
		fmt.Sprintf(`air,location=XiaoMaiDao01,region=uswest temperature=100 %d`, mustParseTime(time.RFC3339Nano, "2009-11-10T23:00:00Z").UnixNano()),
		fmt.Sprintf(`air,location=XiaoMaiDao01,region=useast temperature=100 %d`, mustParseTime(time.RFC3339Nano, "2009-11-10T23:00:00Z").UnixNano()),
		fmt.Sprintf(`air,location=XiaoMaiDao02,region=useast temperature=100 %d`, mustParseTime(time.RFC3339Nano, "2009-11-10T23:00:00Z").UnixNano()),
		fmt.Sprintf(`sea,location=XiaoMaiDao02,region=useast temperature=100 %d`, mustParseTime(time.RFC3339Nano, "2009-11-10T23:00:00Z").UnixNano()),
		fmt.Sprintf(`sea,location=XiaoMaiDao03,region=caeast temperature=100 %d`, mustParseTime(time.RFC3339Nano, "2009-11-10T23:00:00Z").UnixNano()),
		fmt.Sprintf(`wind,location=XiaoMaiDao03,region=caeast temperature=100 %d`, mustParseTime(time.RFC3339Nano, "2009-11-10T23:00:00Z").UnixNano()),
	}

	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}

	test.addQueries([]*Query{
		&Query{
			name:    `show tag key cardinality`,
			command: "SHOW TAG KEY CARDINALITY",
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["count"],"values":[[2]]},{"name":"sea","columns":["count"],"values":[[2]]},{"name":"wind","columns":["count"],"values":[[2]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    `show tag key cardinality on db0`,
			command: "SHOW TAG KEY CARDINALITY ON db0",
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["count"],"values":[[2]]},{"name":"sea","columns":["count"],"values":[[2]]},{"name":"wind","columns":["count"],"values":[[2]]}]}]}`,
		},
		&Query{
			name:    "show tag key cardinality from",
			command: "SHOW TAG KEY CARDINALITY FROM air",
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["count"],"values":[[2]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    "show tag key cardinality from regex",
			command: "SHOW TAG KEY CARDINALITY FROM /air|sea/",
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["count"],"values":[[2]]},{"name":"sea","columns":["count"],"values":[[2]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    "show tag key cardinality measurement not found",
			command: "SHOW TAG KEY CARDINALITY FROM doesntexist",
			exp:     `{"results":[{"statement_id":0}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    "show tag key cardinality with time in WHERE clause errors",
			command: "SHOW TAG KEY CARDINALITY FROM air WHERE time > now() - 1h",
			exp:     `{"results":[{"statement_id":0,"error":"SHOW TAG KEY EXACT CARDINALITY doesn't support time in WHERE clause"}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    `show tag key exact cardinality`,
			command: "SHOW TAG KEY EXACT CARDINALITY",
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["count"],"values":[[2]]},{"name":"sea","columns":["count"],"values":[[2]]},{"name":"wind","columns":["count"],"values":[[2]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    `show tag key exact cardinality on db0`,
			command: "SHOW TAG KEY EXACT CARDINALITY ON db0",
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["count"],"values":[[2]]},{"name":"sea","columns":["count"],"values":[[2]]},{"name":"wind","columns":["count"],"values":[[2]]}]}]}`,
		},
		&Query{
			name:    "show tag key exact cardinality from",
			command: "SHOW TAG KEY EXACT CARDINALITY FROM air",
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["count"],"values":[[2]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    "show tag key exact cardinality from regex",
			command: "SHOW TAG KEY EXACT CARDINALITY FROM /air|sea/",
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["count"],"values":[[2]]},{"name":"sea","columns":["count"],"values":[[2]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    "show tag key exact cardinality measurement not found",
			command: "SHOW TAG KEY EXACT CARDINALITY FROM doesntexist",
			exp:     `{"results":[{"statement_id":0}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    "show tag key exact cardinality with time in WHERE clause errors",
			command: "SHOW TAG KEY EXACT CARDINALITY FROM air WHERE time > now() - 1h",
			exp:     `{"results":[{"statement_id":0,"error":"SHOW TAG KEY EXACT CARDINALITY doesn't support time in WHERE clause"}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    `show tag values cardinality with key and where matches the regular expression`,
			command: `SHOW TAG VALUES CARDINALITY WITH KEY = location WHERE region =~ /ca.*/`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"sea","columns":["count"],"values":[[1]]},{"name":"wind","columns":["count"],"values":[[1]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    `show tag values cardinality with key and where does not match the regular expression`,
			command: `SHOW TAG VALUES CARDINALITY WITH KEY = region WHERE location !~ /XiaoMaiDao0[12]/`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"sea","columns":["count"],"values":[[1]]},{"name":"wind","columns":["count"],"values":[[1]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    `show tag values cardinality with key and where partially matches the regular expression`,
			command: `SHOW TAG VALUES CARDINALITY WITH KEY = location WHERE region =~ /us/`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["count"],"values":[[2]]},{"name":"sea","columns":["count"],"values":[[1]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    `show tag values cardinality with key and where partially does not match the regular expression`,
			command: `SHOW TAG VALUES CARDINALITY WITH KEY = location WHERE region !~ /us/`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"sea","columns":["count"],"values":[[1]]},{"name":"wind","columns":["count"],"values":[[1]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    `show tag values cardinality with key in and where does not match the regular expression`,
			command: `SHOW TAG VALUES CARDINALITY FROM air WITH KEY IN (location, region) WHERE region = 'uswest'`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["count"],"values":[[2]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    `show tag values cardinality with key regex and where does not match the regular expression`,
			command: `SHOW TAG VALUES CARDINALITY FROM air WITH KEY =~ /(location|region)/ WHERE region = 'uswest'`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["count"],"values":[[2]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    `show tag values cardinality with key and measurement matches regular expression`,
			command: `SHOW TAG VALUES CARDINALITY FROM /air|sea/ WITH KEY = location`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["count"],"values":[[2]]},{"name":"sea","columns":["count"],"values":[[2]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    `show tag values exact cardinality with key and where matches the regular expression`,
			command: `SHOW TAG VALUES EXACT CARDINALITY WITH KEY = location WHERE region =~ /ca.*/`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"sea","columns":["count"],"values":[[1]]},{"name":"wind","columns":["count"],"values":[[1]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    `show tag values exact cardinality with key and where does not match the regular expression`,
			command: `SHOW TAG VALUES EXACT CARDINALITY WITH KEY = region WHERE location !~ /XiaoMaiDao0[12]/`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"sea","columns":["count"],"values":[[1]]},{"name":"wind","columns":["count"],"values":[[1]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    `show tag values exact cardinality with key and where partially matches the regular expression`,
			command: `SHOW TAG VALUES EXACT CARDINALITY WITH KEY = location WHERE region =~ /us/`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["count"],"values":[[2]]},{"name":"sea","columns":["count"],"values":[[1]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    `show tag values exact cardinality with key and where partially does not match the regular expression`,
			command: `SHOW TAG VALUES EXACT CARDINALITY WITH KEY = location WHERE region !~ /us/`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"sea","columns":["count"],"values":[[1]]},{"name":"wind","columns":["count"],"values":[[1]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    `show tag values exact cardinality with key in and where does not match the regular expression`,
			command: `SHOW TAG VALUES EXACT CARDINALITY FROM air WITH KEY IN (location, region) WHERE region = 'uswest'`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["count"],"values":[[2]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    `show tag values exact cardinality with key regex and where does not match the regular expression`,
			command: `SHOW TAG VALUES EXACT CARDINALITY FROM air WITH KEY =~ /(location|region)/ WHERE region = 'uswest'`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["count"],"values":[[2]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    `show tag values exact cardinality with key and measurement matches regular expression`,
			command: `SHOW TAG VALUES EXACT CARDINALITY FROM /air|sea/ WITH KEY = location`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["count"],"values":[[2]]},{"name":"sea","columns":["count"],"values":[[2]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
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

func TestServer_Query_ShowFieldKeys(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig())
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("rp0", 1, 0), true); err != nil {
		t.Fatal(err)
	}

	writes := []string{
		fmt.Sprintf(`air,location=XiaoMaiDao01 field1=100 %d`, mustParseTime(time.RFC3339Nano, "2009-11-10T23:00:00Z").UnixNano()),
		fmt.Sprintf(`air,location=XiaoMaiDao01,region=uswest field1=200,field2=300,field3=400 %d`, mustParseTime(time.RFC3339Nano, "2009-11-10T23:00:00Z").UnixNano()),
		fmt.Sprintf(`air,location=XiaoMaiDao01,region=useast field1=200,field2=300,field3=400 %d`, mustParseTime(time.RFC3339Nano, "2009-11-10T23:00:00Z").UnixNano()),
		fmt.Sprintf(`air,location=XiaoMaiDao02,region=useast field1=200,field2=300,field3=400 %d`, mustParseTime(time.RFC3339Nano, "2009-11-10T23:00:00Z").UnixNano()),
		fmt.Sprintf(`sea,location=XiaoMaiDao01,region=useast field4=200,field5=300 %d`, mustParseTime(time.RFC3339Nano, "2009-11-10T23:00:00Z").UnixNano()),
		fmt.Sprintf(`sea,location=XiaoMaiDao03,region=caeast field6=200,field7=300 %d`, mustParseTime(time.RFC3339Nano, "2009-11-10T23:00:00Z").UnixNano()),
		fmt.Sprintf(`wind,location=XiaoMaiDao03,region=caeast field8=200,field9=300 %d`, mustParseTime(time.RFC3339Nano, "2009-11-10T23:00:00Z").UnixNano()),
	}

	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}

	test.addQueries([]*Query{
		&Query{
			name:    `show field keys`,
			command: `SHOW FIELD KEYS`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["fieldKey","fieldType"],"values":[["field1","float"],["field2","float"],["field3","float"]]},{"name":"sea","columns":["fieldKey","fieldType"],"values":[["field4","float"],["field5","float"],["field6","float"],["field7","float"]]},{"name":"wind","columns":["fieldKey","fieldType"],"values":[["field8","float"],["field9","float"]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    `show field keys from measurement`,
			command: `SHOW FIELD KEYS FROM air`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["fieldKey","fieldType"],"values":[["field1","float"],["field2","float"],["field3","float"]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    `show field keys measurement with regex`,
			command: `SHOW FIELD KEYS FROM /air|sea/`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["fieldKey","fieldType"],"values":[["field1","float"],["field2","float"],["field3","float"]]},{"name":"sea","columns":["fieldKey","fieldType"],"values":[["field4","float"],["field5","float"],["field6","float"],["field7","float"]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
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

func TestServer_Query_ShowFieldKeyCardinality(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig())
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("rp0", 1, 0), true); err != nil {
		t.Fatal(err)
	}

	writes := []string{
		fmt.Sprintf(`air,location=XiaoMaiDao01 field1=100 %d`, mustParseTime(time.RFC3339Nano, "2009-11-10T23:00:00Z").UnixNano()),
		fmt.Sprintf(`air,location=XiaoMaiDao01,region=uswest field1=200,field2=300,field3=400 %d`, mustParseTime(time.RFC3339Nano, "2009-11-10T23:00:00Z").UnixNano()),
		fmt.Sprintf(`air,location=XiaoMaiDao01,region=useast field1=200,field2=300,field3=400 %d`, mustParseTime(time.RFC3339Nano, "2009-11-10T23:00:00Z").UnixNano()),
		fmt.Sprintf(`air,location=XiaoMaiDao02,region=useast field1=200,field2=300,field3=400 %d`, mustParseTime(time.RFC3339Nano, "2009-11-10T23:00:00Z").UnixNano()),
		fmt.Sprintf(`sea,location=XiaoMaiDao01,region=useast field4=200,field5=300 %d`, mustParseTime(time.RFC3339Nano, "2009-11-10T23:00:00Z").UnixNano()),
		fmt.Sprintf(`sea,location=XiaoMaiDao03,region=caeast field6=200,field7=300 %d`, mustParseTime(time.RFC3339Nano, "2009-11-10T23:00:00Z").UnixNano()),
		fmt.Sprintf(`wind,location=XiaoMaiDao03,region=caeast field8=200,field9=300 %d`, mustParseTime(time.RFC3339Nano, "2009-11-10T23:00:00Z").UnixNano()),
	}

	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}

	test.addQueries([]*Query{
		&Query{
			name:    `show field key cardinality`,
			command: `SHOW FIELD KEY CARDINALITY`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["count"],"values":[[3]]},{"name":"sea","columns":["count"],"values":[[4]]},{"name":"wind","columns":["count"],"values":[[2]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    `show field key cardinality from measurement`,
			command: `SHOW FIELD KEY CARDINALITY FROM air`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["count"],"values":[[3]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    `show field key cardinality measurement with regex`,
			command: `SHOW FIELD KEY CARDINALITY FROM /air|sea/`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["count"],"values":[[3]]},{"name":"sea","columns":["count"],"values":[[4]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    `show field key exact cardinality`,
			command: `SHOW FIELD KEY EXACT CARDINALITY`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["count"],"values":[[3]]},{"name":"sea","columns":["count"],"values":[[4]]},{"name":"wind","columns":["count"],"values":[[2]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    `show field key exact cardinality from measurement`,
			command: `SHOW FIELD KEY EXACT CARDINALITY FROM air`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["count"],"values":[[3]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    `show field key exact cardinality measurement with regex`,
			command: `SHOW FIELD KEY EXACT CARDINALITY FROM /air|sea/`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["count"],"values":[[3]]},{"name":"sea","columns":["count"],"values":[[4]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
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

// Tests that a known CQ query with concurrent writes does not deadlock the server

func TestServer_Query_EvilIdentifiers(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig())
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("rp0", 1, 0), true); err != nil {
		t.Fatal(err)
	}

	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: fmt.Sprintf("air select=1,in-bytes=2 %d", mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00Z").UnixNano())},
	}

	test.addQueries([]*Query{
		&Query{
			name:    `query evil identifiers`,
			command: `SELECT "select", "in-bytes" FROM air`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","select","in-bytes"],"values":[["2000-01-01T00:00:00Z",1,2]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
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

func TestServer_Query_OrderByTime(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig())
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("rp0", 1, 0), true); err != nil {
		t.Fatal(err)
	}

	writes := []string{
		fmt.Sprintf(`air,station=XiaoMaiDao1 temperature=1 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:01Z").UnixNano()),
		fmt.Sprintf(`air,station=XiaoMaiDao1 temperature=2 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:02Z").UnixNano()),
		fmt.Sprintf(`air,station=XiaoMaiDao1 temperature=3 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:03Z").UnixNano()),

		fmt.Sprintf(`sea,presence=true temperature=1 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:01Z").UnixNano()),
		fmt.Sprintf(`sea,presence=true temperature=2 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:02Z").UnixNano()),
		fmt.Sprintf(`sea,presence=true temperature=3 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:03Z").UnixNano()),
		fmt.Sprintf(`sea,presence=false temperature=4 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:04Z").UnixNano()),

		fmt.Sprintf(`wind,station=XiaoMaiDao1 free=1 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:01Z").UnixNano()),
		fmt.Sprintf(`wind,station=XiaoMaiDao1 free=2 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:02Z").UnixNano()),
		fmt.Sprintf(`wind,station=XiaoMaiDao2 used=3 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:01Z").UnixNano()),
		fmt.Sprintf(`wind,station=XiaoMaiDao2 used=4 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:02Z").UnixNano()),
	}

	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}

	test.addQueries([]*Query{
		&Query{
			name:    "order on points",
			params:  url.Values{"db": []string{"db0"}},
			command: `select temperature from "air" ORDER BY time DESC`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","temperature"],"values":[["2000-01-01T00:00:03Z",3],["2000-01-01T00:00:02Z",2],["2000-01-01T00:00:01Z",1]]}]}]}`,
		},

		&Query{
			name:    "order desc with tags",
			params:  url.Values{"db": []string{"db0"}},
			command: `select temperature from "sea" ORDER BY time DESC`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"sea","columns":["time","temperature"],"values":[["2000-01-01T00:00:04Z",4],["2000-01-01T00:00:03Z",3],["2000-01-01T00:00:02Z",2],["2000-01-01T00:00:01Z",1]]}]}]}`,
		},

		&Query{
			name:    "order desc with sparse data",
			params:  url.Values{"db": []string{"db0"}},
			command: `select used, free from "wind" ORDER BY time DESC`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"wind","columns":["time","used","free"],"values":[["2000-01-01T00:00:02Z",4,null],["2000-01-01T00:00:02Z",null,2],["2000-01-01T00:00:01Z",3,null],["2000-01-01T00:00:01Z",null,1]]}]}]}`,
		},

		&Query{
			name:    "order desc with an aggregate and sparse data",
			params:  url.Values{"db": []string{"db0"}},
			command: `select first("used") AS "used", first("free") AS "free" from "wind" WHERE time >= '2000-01-01T00:00:01Z' AND time <= '2000-01-01T00:00:02Z' GROUP BY station, time(1s) FILL(none) ORDER BY time DESC`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"wind","tags":{"station":"XiaoMaiDao2"},"columns":["time","used","free"],"values":[["2000-01-01T00:00:02Z",4,null],["2000-01-01T00:00:01Z",3,null]]},{"name":"wind","tags":{"station":"XiaoMaiDao1"},"columns":["time","used","free"],"values":[["2000-01-01T00:00:02Z",null,2],["2000-01-01T00:00:01Z",null,1]]}]}]}`,
		},
	}...)

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

func TestServer_Query_FieldWithMultiplePeriods(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig())
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("rp0", 1, 0), true); err != nil {
		t.Fatal(err)
	}

	writes := []string{
		fmt.Sprintf(`air foo.bar.baz=1 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00Z").UnixNano()),
	}

	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}

	test.addQueries([]*Query{
		&Query{
			name:    "baseline",
			params:  url.Values{"db": []string{"db0"}},
			command: `select * from air`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","foo.bar.baz"],"values":[["2000-01-01T00:00:00Z",1]]}]}]}`,
		},
		&Query{
			name:    "select field with periods",
			params:  url.Values{"db": []string{"db0"}},
			command: `select "foo.bar.baz" from air`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","foo.bar.baz"],"values":[["2000-01-01T00:00:00Z",1]]}]}]}`,
		},
	}...)

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

func TestServer_Query_FieldWithMultiplePeriodsMeasurementPrefixMatch(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig())
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("rp0", 1, 0), true); err != nil {
		t.Fatal(err)
	}

	writes := []string{
		fmt.Sprintf(`foo foo.bar.baz=1 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00Z").UnixNano()),
	}

	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}

	test.addQueries([]*Query{
		&Query{
			name:    "baseline",
			params:  url.Values{"db": []string{"db0"}},
			command: `select * from foo`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"foo","columns":["time","foo.bar.baz"],"values":[["2000-01-01T00:00:00Z",1]]}]}]}`,
		},
		&Query{
			name:    "select field with periods",
			params:  url.Values{"db": []string{"db0"}},
			command: `select "foo.bar.baz" from foo`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"foo","columns":["time","foo.bar.baz"],"values":[["2000-01-01T00:00:00Z",1]]}]}]}`,
		},
	}...)

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

func TestServer_Query_IntoTarget(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig())
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("rp0", 1, 0), true); err != nil {
		t.Fatal(err)
	}

	writes := []string{
		fmt.Sprintf(`foo value=1 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00Z").UnixNano()),
		fmt.Sprintf(`foo value=2 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:10Z").UnixNano()),
		fmt.Sprintf(`foo value=3 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:20Z").UnixNano()),
		fmt.Sprintf(`foo value=4 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:30Z").UnixNano()),
		fmt.Sprintf(`foo value=4,foobar=3 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:40Z").UnixNano()),
	}

	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}

	test.addQueries([]*Query{
		&Query{
			name:    "into",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT * INTO baz FROM foo`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"result","columns":["time","written"],"values":[["1970-01-01T00:00:00Z",5]]}]}]}`,
		},
		&Query{
			name:    "confirm results",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT * FROM baz`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"baz","columns":["time","foobar","value"],"values":[["2000-01-01T00:00:00Z",null,1],["2000-01-01T00:00:10Z",null,2],["2000-01-01T00:00:20Z",null,3],["2000-01-01T00:00:30Z",null,4],["2000-01-01T00:00:40Z",3,4]]}]}]}`,
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

func TestServer_Query_IntoTarget_Sparse(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig())
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("rp0", 1, 0), true); err != nil {
		t.Fatal(err)
	}

	writes := []string{
		fmt.Sprintf(`fun a=2,n=3 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:01Z").UnixNano()),
		fmt.Sprintf(`fun a=5,n=7 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:02Z").UnixNano()),
		fmt.Sprintf(`fun a=11,b=13,n=17 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:11Z").UnixNano()),
	}

	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}

	test.addQueries([]*Query{
		&Query{
			name:    "into",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT sum(a) * sum(n) as a_n, sum(b) * sum(n) as b_n INTO baz FROM fun WHERE time >= '2000-01-01T00:00:00Z' AND time < '2000-01-01T00:01:00Z' GROUP BY time(10s)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"result","columns":["time","written"],"values":[["1970-01-01T00:00:00Z",2]]}]}]}`,
		},
		&Query{
			name:    "confirm results",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT * FROM baz`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"baz","columns":["time","a_n","b_n"],"values":[["2000-01-01T00:00:00Z",70,null],["2000-01-01T00:00:10Z",187,221]]}]}]}`,
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

func TestServer_Query_DuplicateMeasurements(t *testing.T) {
	t.Parallel()
	s := OpenDefaultServer(NewConfig())
	defer s.Close()

	// Create a second database.
	if err := s.CreateDatabaseAndRetentionPolicy("db1", NewRetentionPolicySpec("rp0", 1, 0), true); err != nil {
		t.Fatal(err)
	}

	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: fmt.Sprintf(`air temperature=1 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00Z").UnixNano())},
	}

	if err := test.init(s); err != nil {
		t.Fatalf("test init failed: %s", err)
	}

	test = NewTest("db1", "rp0")
	test.writes = Writes{
		&Write{data: fmt.Sprintf(`air temperature=2 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:10Z").UnixNano())},
	}

	if err := test.init(s); err != nil {
		t.Fatalf("test init failed: %s", err)
	}

	test.addQueries([]*Query{
		&Query{
			name:    "select from both databases",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT temperature FROM db0.rp0.air, db1.rp0.air`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","temperature"],"values":[["2000-01-01T00:00:00Z",1],["2000-01-01T00:00:10Z",2]]}]}]}`,
		},
	}...)

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

func TestServer_Query_LargeTimestamp(t *testing.T) {
	t.Parallel()
	s := OpenDefaultServer(NewConfig())
	defer s.Close()

	if _, ok := s.(*RemoteServer); ok {
		t.Skip("Skipping.  Cannot restart remote server")
	}

	writes := []string{
		fmt.Sprintf(`air temperature=100 %d`, models.MaxNanoTime),
	}

	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}

	test.addQueries([]*Query{
		&Query{
			name:    `select temperature at max nano time`,
			params:  url.Values{"db": []string{"db0"}},
			command: fmt.Sprintf(`SELECT temperature FROM air WHERE time <= %d`, models.MaxNanoTime),
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","temperature"],"values":[["` + time.Unix(0, models.MaxNanoTime).UTC().Format(time.RFC3339Nano) + `",100]]}]}]}`,
		},
	}...)

	if err := test.init(s); err != nil {
		t.Fatalf("test init failed: %s", err)
	}

	// Open a new server with the same configuration file.
	// This is to ensure the meta data was marshaled correctly.
	s2 := OpenServer((s.(*LocalServer)).Config)
	defer s2.(*LocalServer).Server.Close()

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

func TestServer_Query_DotProduct(t *testing.T) {
	t.Parallel()
	s := OpenDefaultServer(NewConfig())
	defer s.Close()

	// Create a second database.
	if err := s.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("rp0", 1, 0), true); err != nil {
		t.Fatal(err)
	}

	writes := []string{
		fmt.Sprintf(`air a=2,b=3 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00Z").UnixNano()),
		fmt.Sprintf(`air a=-5,b=8 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:10Z").UnixNano()),
		fmt.Sprintf(`air a=9,b=3 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:20Z").UnixNano()),
	}

	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}

	if err := test.init(s); err != nil {
		t.Fatalf("test init failed: %s", err)
	}

	test.addQueries([]*Query{
		&Query{
			name:    "select dot product",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT sum(a_b) FROM (SELECT a * b FROM air) WHERE time >= '2000-01-01T00:00:00Z' AND time < '2000-01-01T00:00:30Z'`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","sum"],"values":[["2000-01-01T00:00:00Z",-7]]}]}]}`,
		},
	}...)

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

func TestServer_ConcurrentPointsWriter_Subscriber(t *testing.T) {
	t.Parallel()
	s := OpenDefaultServer(NewConfig())
	defer s.Close()

	if _, ok := s.(*RemoteServer); ok {
		t.Skip("Skipping.  Cannot access PointsWriter remotely")
	}
	// goroutine to write points
	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-done:
				return
			default:
				wpr := &coordinator.WritePointsRequest{
					Database:        "db0",
					RetentionPolicy: "rp0",
				}
				s.WritePoints(wpr.Database, wpr.RetentionPolicy, models.ConsistencyLevelAny, nil, wpr.Points)
			}
		}
	}()

	time.Sleep(10 * time.Millisecond)

	close(done)
	wg.Wait()
}

func TestServer_WhereTimeInclusive(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig())
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("rp0", 1, 0), true); err != nil {
		t.Fatal(err)
	}

	writes := []string{
		fmt.Sprintf(`air temperature=1 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:01Z").UnixNano()),
		fmt.Sprintf(`air temperature=2 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:02Z").UnixNano()),
		fmt.Sprintf(`air temperature=3 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:03Z").UnixNano()),
	}

	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}

	test.addQueries([]*Query{
		&Query{
			name:    "all GTE/LTE",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT * from air where time >= '2000-01-01T00:00:01Z' and time <= '2000-01-01T00:00:03Z'`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","temperature"],"values":[["2000-01-01T00:00:01Z",1],["2000-01-01T00:00:02Z",2],["2000-01-01T00:00:03Z",3]]}]}]}`,
		},
		&Query{
			name:    "all GTE",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT * from air where time >= '2000-01-01T00:00:01Z'`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","temperature"],"values":[["2000-01-01T00:00:01Z",1],["2000-01-01T00:00:02Z",2],["2000-01-01T00:00:03Z",3]]}]}]}`,
		},
		&Query{
			name:    "all LTE",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT * from air where time <= '2000-01-01T00:00:03Z'`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","temperature"],"values":[["2000-01-01T00:00:01Z",1],["2000-01-01T00:00:02Z",2],["2000-01-01T00:00:03Z",3]]}]}]}`,
		},
		&Query{
			name:    "first GTE/LTE",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT * from air where time >= '2000-01-01T00:00:01Z' and time <= '2000-01-01T00:00:01Z'`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","temperature"],"values":[["2000-01-01T00:00:01Z",1]]}]}]}`,
		},
		&Query{
			name:    "last GTE/LTE",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT * from air where time >= '2000-01-01T00:00:03Z' and time <= '2000-01-01T00:00:03Z'`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","temperature"],"values":[["2000-01-01T00:00:03Z",3]]}]}]}`,
		},
		&Query{
			name:    "before GTE/LTE",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT * from air where time <= '2000-01-01T00:00:00Z'`,
			exp:     `{"results":[{"statement_id":0}]}`,
		},
		&Query{
			name:    "all GT/LT",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT * from air where time > '2000-01-01T00:00:00Z' and time < '2000-01-01T00:00:04Z'`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","temperature"],"values":[["2000-01-01T00:00:01Z",1],["2000-01-01T00:00:02Z",2],["2000-01-01T00:00:03Z",3]]}]}]}`,
		},
		&Query{
			name:    "first GT/LT",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT * from air where time > '2000-01-01T00:00:00Z' and time < '2000-01-01T00:00:02Z'`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","temperature"],"values":[["2000-01-01T00:00:01Z",1]]}]}]}`,
		},
		&Query{
			name:    "last GT/LT",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT * from air where time > '2000-01-01T00:00:02Z' and time < '2000-01-01T00:00:04Z'`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","temperature"],"values":[["2000-01-01T00:00:03Z",3]]}]}]}`,
		},
		&Query{
			name:    "all GT",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT * from air where time > '2000-01-01T00:00:00Z'`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","temperature"],"values":[["2000-01-01T00:00:01Z",1],["2000-01-01T00:00:02Z",2],["2000-01-01T00:00:03Z",3]]}]}]}`,
		},
		&Query{
			name:    "all LT",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT * from air where time < '2000-01-01T00:00:04Z'`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","temperature"],"values":[["2000-01-01T00:00:01Z",1],["2000-01-01T00:00:02Z",2],["2000-01-01T00:00:03Z",3]]}]}]}`,
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

func TestServer_Query_Sample_Wildcard(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig())
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("rp0", 1, 0), true); err != nil {
		t.Fatal(err)
	}

	writes := []string{
		fmt.Sprintf(`air float=1,int=1i,string="hello, cnosdb",bool=true %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00Z").UnixNano()),
	}

	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}

	test.addQueries([]*Query{
		&Query{
			name:    "sample() with wildcard",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT sample(*, 1) FROM air`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","sample_bool","sample_float","sample_int","sample_string"],"values":[["2000-01-01T00:00:00Z",true,1,1,"hello, cnosdb"]]}]}]}`,
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

func TestServer_Query_Sample_LimitOffset(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig())
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("rp0", 1, 0), true); err != nil {
		t.Fatal(err)
	}

	writes := []string{
		fmt.Sprintf(`air float=1,int=1i %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00Z").UnixNano()),
		fmt.Sprintf(`air float=2,int=2i %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:01:00Z").UnixNano()),
		fmt.Sprintf(`air float=3,int=3i %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:02:00Z").UnixNano()),
	}

	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}

	test.addQueries([]*Query{
		&Query{
			name:    "sample() with limit 1",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT sample(float, 3), int FROM air LIMIT 1`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","sample","int"],"values":[["2000-01-01T00:00:00Z",1,1]]}]}]}`,
		},
		&Query{
			name:    "sample() with offset 1",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT sample(float, 3), int FROM air OFFSET 1`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","sample","int"],"values":[["2000-01-01T00:01:00Z",2,2],["2000-01-01T00:02:00Z",3,3]]}]}]}`,
		},
		&Query{
			name:    "sample() with limit 1 offset 1",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT sample(float, 3), int FROM air LIMIT 1 OFFSET 1`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","sample","int"],"values":[["2000-01-01T00:01:00Z",2,2]]}]}]}`,
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

func TestServer_NestedAggregateWithMathPanics(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig())
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("rp0", 1, 0), true); err != nil {
		t.Fatal(err)
	}

	writes := []string{
		`air temperature=2i 120000000000`,
	}

	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}

	test.addQueries([]*Query{
		&Query{
			name:    "dividing by elapsed count should not panic",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT sum(temperature) / elapsed(sum(temperature), 1m) FROM air WHERE time > 0 AND time < 10m GROUP BY time(1m)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","sum_elapsed"],"values":[["1970-01-01T00:00:00Z",null],["1970-01-01T00:01:00Z",null],["1970-01-01T00:02:00Z",null],["1970-01-01T00:03:00Z",null],["1970-01-01T00:04:00Z",null],["1970-01-01T00:05:00Z",null],["1970-01-01T00:06:00Z",null],["1970-01-01T00:07:00Z",null],["1970-01-01T00:08:00Z",null],["1970-01-01T00:09:00Z",null]]}]}]}`,
		},
		&Query{
			name:    "dividing by elapsed count with fill previous should not panic",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT sum(temperature) / elapsed(sum(temperature), 1m) FROM air WHERE time > 0 AND time < 10m GROUP BY time(1m) FILL(previous)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"air","columns":["time","sum_elapsed"],"values":[["1970-01-01T00:00:00Z",null],["1970-01-01T00:01:00Z",null],["1970-01-01T00:02:00Z",null],["1970-01-01T00:03:00Z",2],["1970-01-01T00:04:00Z",2],["1970-01-01T00:05:00Z",2],["1970-01-01T00:06:00Z",2],["1970-01-01T00:07:00Z",2],["1970-01-01T00:08:00Z",2],["1970-01-01T00:09:00Z",2]]}]}]}`,
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

func TestServer_Prometheus_Read(t *testing.T) {
	//TODO: Prometheus is not yet
}

func TestServer_Prometheus_Write(t *testing.T) {
	//TODO: Prometheus is not yet
}

// support for uint
func init() {
	models.EnableUintSupport()
}
