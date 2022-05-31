package meta

import (
	"io/ioutil"
	"os"
	"path"
	"runtime"
	"testing"
	"time"
)

func TestMetaClient_CreateDatabaseOnly(t *testing.T) {
	t.Parallel()

	dir, c := newClient()

	defer os.RemoveAll(dir)
	defer c.Close()

	if db, err := c.CreateDatabase("db0"); err != nil {
		t.Fatal(err)
	} else if db.Name != "db0" {
		t.Fatalf("database name mismatch.  exp: db0, got %s", db.Name)
	}

	db := c.Database("db0")
	if db == nil {
		t.Fatal("database is not existing")
	} else if db.Name != "db0" {
		t.Fatalf("db name is wrong.")
	}

	rp, err := c.RetentionPolicy("db0", "autogen")
	if err != nil {
		t.Fatal(err)
	} else if rp == nil {
		t.Fatalf("retention policy is not existing.")
	} else if rp.Name != "autogen" {
		t.Fatalf("retention policy mismatch. exp:autogen, got %s", rp.Name)
	}
}

func TestMetaClient_CreateDatabaseIfNotExists(t *testing.T) {
	t.Parallel()

	dir, c := newClient()
	defer os.RemoveAll(dir)
	defer c.Close()

	if _, err := c.CreateDatabase("db0"); err != nil {
		t.Fatal(err)
	}

	db := c.Database("db0")
	if db == nil {
		t.Fatal("database not found")
	} else if db.Name != "db0" {
		t.Fatalf("db name wrong: %s", db.Name)
	}

	if _, err := c.CreateDatabase("db0"); err != nil {
		t.Fatal(err)
	}

	rp, err := c.RetentionPolicy("db0", "autogen")
	if err != nil {
		t.Fatal(err)
	} else if rp == nil {
		t.Fatalf("retention policy is not existing.")
	} else if rp.Name != "autogen" {
		t.Fatalf("retention policy mismatch. exp:autogen, got %s", rp.Name)
	}
}

func TestMetaClient_CreateDatabaseWithRetentionPolicy(t *testing.T) {
	t.Parallel()

	dir, c := newClient()
	defer os.RemoveAll(dir)
	defer c.Close()

	if _, err := c.CreateDatabaseWithRetentionPolicy("db0", nil); err == nil {
		t.Fatal("expected error")
	}

	duration := 1 * time.Hour
	replicaN := 1
	spec := RetentionPolicySpec{
		Name:               "rp0",
		Duration:           &duration,
		ReplicaN:           &replicaN,
		ShardGroupDuration: 60 * time.Minute,
	}
	if _, err := c.CreateDatabaseWithRetentionPolicy("db0", &spec); err != nil {
		t.Fatal(err)
	}

	db := c.Database("db0")
	if db == nil {
		t.Fatal("database not found")
	} else if db.Name != "db0" {
		t.Fatalf("db name wrong: %s", db.Name)
	}

	rp := db.RetentionPolicy("rp0")
	if rp.Name != "rp0" {
		t.Fatalf("rp name wrong: %s", rp.Name)
	} else if rp.Duration != time.Hour {
		t.Fatalf("rp duration wrong: %v", rp.Duration)
	} else if rp.ReplicaN != 1 {
		t.Fatalf("rp replication wrong: %d", rp.ReplicaN)
	} else if rp.ShardGroupDuration != 60*time.Minute {
		t.Fatalf("rp shard duration wrong: %v", rp.ShardGroupDuration)
	}

	if _, err := c.CreateDatabaseWithRetentionPolicy("db0", &spec); err != nil {
		t.Fatal(err)
	}

	if db0, err := c.CreateDatabase("db0"); err != nil {
		t.Fatalf("got %v, but exp %v", err, nil)
	} else if db0.DefaultRetentionPolicy != "rp0" {
		t.Fatalf("got %v, but exp %v", db0.DefaultRetentionPolicy, "rp0")
	} else if got, exp := len(db0.RetentionPolicies), 1; got != exp {
		t.Fatalf("got %v, but exp %v", got, exp)
	}
}







func newClient() (string, *Client) {
	config := newConfig()
	c := NewClient(config)
	if err := c.Open(); err != nil {
		panic(err)
	}
	return config.Dir, c
}

func newConfig() *Config {
	cfg := NewConfig()
	cfg.Dir = makeTempDir(2)
	return cfg
}

func makeTempDir(skip int) string {
	var pc, _, _, ok = runtime.Caller(skip)
	if !ok {
		panic("failed to get name of test function")
	}
	_, prefix := path.Split(runtime.FuncForPC(pc).Name())

	dir, err := ioutil.TempDir(os.TempDir(), prefix)
	if err != nil {
		panic(err)
	}
	return dir
}

func isAdmin(u User) bool {
	return u.(*UserInfo).Admin
}
