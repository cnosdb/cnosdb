package meta

import (
	"github.com/cnosdb/cnosdb/vend/cnosql"
	"io/ioutil"
	"os"
	"path"
	"reflect"
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

func TestMetaClient_CreateDatabaseWithRetentionPolicy_Conflict_Fields(t *testing.T) {
	t.Parallel()

	dir, c := newClient()
	defer os.RemoveAll(dir)
	defer c.Close()

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

	spec2 := spec
	spec2.Name = spec.Name + "1"
	if _, err := c.CreateDatabaseWithRetentionPolicy("db0", &spec2); err != ErrRetentionPolicyConflict {
		t.Fatalf("got %v, but expected %v", err, ErrRetentionPolicyConflict)
	}

	spec2 = spec
	duration2 := *spec.Duration + time.Minute
	spec2.Duration = &duration2
	if _, err := c.CreateDatabaseWithRetentionPolicy("db0", &spec2); err != ErrRetentionPolicyConflict {
		t.Fatalf("got %v, but expected %v", err, ErrRetentionPolicyConflict)
	}

	spec2 = spec
	replica2 := *spec.ReplicaN + 1
	spec2.ReplicaN = &replica2
	if _, err := c.CreateDatabaseWithRetentionPolicy("db0", &spec2); err != ErrRetentionPolicyConflict {
		t.Fatalf("got %v, but expected %v", err, ErrRetentionPolicyConflict)
	}

	spec2 = spec
	spec2.ShardGroupDuration = spec.ShardGroupDuration + time.Minute
	if _, err := c.CreateDatabaseWithRetentionPolicy("db0", &spec2); err != ErrRetentionPolicyConflict {
		t.Fatalf("got %v, but expected %v", err, ErrRetentionPolicyConflict)
	}
}

func TestMetaClient_CreateDatabaseWithRetentionPolicy_Conflict_NonDefault(t *testing.T) {
	t.Parallel()

	d, c := newClient()
	defer os.RemoveAll(d)
	defer c.Close()

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

	spec2 := spec
	spec2.Name = "rp1"
	if _, err := c.CreateRetentionPolicy("db0", &spec2, false); err != nil {
		t.Fatal(err)
	}

	//notice that makeDefault field is not set,that's error.
	if _, err := c.CreateDatabaseWithRetentionPolicy("db0", &spec2); err != ErrRetentionPolicyConflict {
		t.Fatalf("got %v, but expected %v", err, ErrRetentionPolicyConflict)
	}
}

func TestMetaClient_Databases(t *testing.T) {
	t.Parallel()

	dir, c := newClient()
	defer os.RemoveAll(dir)
	defer c.Close()

	// Create three databases.
	db, err := c.CreateDatabase("db0")
	if err != nil {
		t.Fatal(err)
	} else if db == nil {
		t.Fatal("database not found")
	} else if db.Name != "db0" {
		t.Fatalf("db name wrong: %s", db.Name)
	}

	db, err = c.CreateDatabase("db1")
	if err != nil {
		t.Fatal(err)
	} else if db.Name != "db1" {
		t.Fatalf("db name wrong: %s", db.Name)
	}

	db, err = c.CreateDatabase("db2")
	if err != nil {
		t.Fatal(err)
	} else if db.Name != "db2" {
		t.Fatalf("db name wrong: %s", db.Name)
	}

	dbs := c.Databases()
	if err != nil {
		t.Fatal(err)
	}
	if len(dbs) != 3 {
		t.Fatalf("expected 2 databases but got %d", len(dbs))
	} else if dbs[0].Name != "db0" {
		t.Fatalf("db name wrong: %s", dbs[0].Name)
	} else if dbs[1].Name != "db1" {
		t.Fatalf("db name wrong: %s", dbs[1].Name)
	} else if dbs[2].Name != "db2" {
		t.Fatalf("db name wrong: %s", dbs[2].Name)
	}
}

func TestMetaClient_DropDatabase(t *testing.T) {
	t.Parallel()

	dir, c := newClient()
	defer os.RemoveAll(dir)
	defer c.Close()

	if _, err := c.CreateDatabase("db0"); err != nil {
		t.Fatal(err)
	}

	db := c.Database("db0")
	if db == nil {
		t.Fatalf("database not found")
	} else if db.Name != "db0" {
		t.Fatalf("db name wrong: %s", db.Name)
	}

	if err := c.DropDatabase("db0"); err != nil {
		t.Fatal(err)
	}

	if db = c.Database("db0"); db != nil {
		t.Fatalf("expected database to no return: %v", db)
	}

	if err := c.DropDatabase("db foo"); err != nil {
		t.Fatalf("got %v error, but expected no error", err)
	}
}

func TestMetaClient_CreateRetentionPolicy(t *testing.T) {
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

	rp0 := RetentionPolicyInfo{
		Name:               "rp0",
		ReplicaN:           1,
		Duration:           2 * time.Hour,
		ShardGroupDuration: 2 * time.Hour,
	}

	rp0Spec := &RetentionPolicySpec{
		Name:               rp0.Name,
		ReplicaN:           &rp0.ReplicaN,
		Duration:           &rp0.Duration,
		ShardGroupDuration: rp0.ShardGroupDuration,
	}

	if _, err := c.CreateRetentionPolicy("db0", rp0Spec, true); err != nil {
		t.Fatal(err)
	}

	actual, err := c.RetentionPolicy("db0", "rp0")
	if err != nil {
		t.Fatal(err)
	} else if got, exp := actual, &rp0; !reflect.DeepEqual(got, exp) {
		t.Fatalf("got %#v, expected %#v", got, exp)
	}

	if _, err := c.CreateRetentionPolicy("db0", rp0Spec, true); err != nil {
		t.Fatal(err)
	} else if actual, err = c.RetentionPolicy("db0", "rp0"); err != nil {
		t.Fatal(err)
	} else if got, exp := actual, &rp0; !reflect.DeepEqual(got, exp) {
		t.Fatalf("got %#v, expected %#v", got, exp)
	}

	rp1 := rp0
	rp1.Duration = 2 * rp0.Duration

	rp1Spec := &RetentionPolicySpec{
		Name:               rp1.Name,
		ReplicaN:           &rp1.ReplicaN,
		Duration:           &rp1.Duration,
		ShardGroupDuration: rp1.ShardGroupDuration,
	}
	_, got := c.CreateRetentionPolicy("db0", rp1Spec, true)
	if exp := ErrRetentionPolicyExists; got != exp {
		t.Fatalf("got error %v, expected error %v", got, exp)
	}

	rp1Spec = rp0Spec
	*rp1Spec.ReplicaN = *rp0Spec.ReplicaN + 1

	_, got = c.CreateRetentionPolicy("db0", rp1Spec, true)
	if exp := ErrRetentionPolicyExists; got != exp {
		t.Fatalf("got error %v, expected error %v", got, exp)
	}

	rp1Spec = rp0Spec
	rp1Spec.ShardGroupDuration = rp0Spec.ShardGroupDuration / 2

	_, got = c.CreateRetentionPolicy("db0", rp1Spec, true)
	if exp := ErrRetentionPolicyExists; got != exp {
		t.Fatalf("got error %v, expected error %v", got, exp)
	}

	rp1Spec = rp0Spec
	*rp1Spec.Duration = 1 * time.Hour
	rp1Spec.ShardGroupDuration = 2 * time.Hour

	_, got = c.CreateRetentionPolicy("db0", rp1Spec, true)
	if exp := ErrIncompatibleDurations; got != exp {
		t.Fatalf("got error %v, expected error %v", got, exp)
	}
}

func TestMetaClient_DefaultRetentionPolicy(t *testing.T) {
	t.Parallel()

	dir, c := newClient()
	defer os.RemoveAll(dir)
	defer c.Close()

	duration := 1 * time.Hour
	replicaN := 1
	if _, err := c.CreateDatabaseWithRetentionPolicy("db0", &RetentionPolicySpec{
		Name:     "rp0",
		Duration: &duration,
		ReplicaN: &replicaN,
	}); err != nil {
		t.Fatal(err)
	}

	db := c.Database("db0")
	if db == nil {
		t.Fatal("database not found")
	} else if db.Name != "db0" {
		t.Fatalf("db name wrong: %s", db.Name)
	}

	rp, err := c.RetentionPolicy("db0", "rp0")
	if err != nil {
		t.Fatal(err)
	} else if rp.Name != "rp0" {
		t.Fatalf("rp name wrong: %s", rp.Name)
	} else if rp.Duration != time.Hour {
		t.Fatalf("rp duration wrong: %s", rp.Duration.String())
	} else if rp.ReplicaN != 1 {
		t.Fatalf("rp replication wrong: %d", rp.ReplicaN)
	}

	if exp, got := "rp0", db.DefaultRetentionPolicy; exp != got {
		t.Fatalf("rp name wrong: \n\texp: %s\n\tgot: %s", exp, db.DefaultRetentionPolicy)
	}
}

func TestMetaClient_UpdateRetentionPolicy(t *testing.T) {
	t.Parallel()

	d, c := newClient()
	defer os.RemoveAll(d)
	defer c.Close()

	if _, err := c.CreateDatabaseWithRetentionPolicy("db0", &RetentionPolicySpec{
		Name:               "rp0",
		ShardGroupDuration: 4 * time.Hour,
	}); err != nil {
		t.Fatal(err)
	}

	rpi, err := c.RetentionPolicy("db0", "rp0")
	if err != nil {
		t.Fatal(err)
	}

	duration := 2 * rpi.ShardGroupDuration
	replicaN := 1
	if err := c.UpdateRetentionPolicy("db0", "rp0", &RetentionPolicyUpdate{
		Duration: &duration,
		ReplicaN: &replicaN,
	}, true); err != nil {
		t.Fatal(err)
	}

	rpi, err = c.RetentionPolicy("db0", "rp0")
	if err != nil {
		t.Fatal(err)
	}
	if exp, got := 4*time.Hour, rpi.ShardGroupDuration; exp != got {
		t.Fatalf("shard group duration wrong: \n\texp: %s\n\tgot: %s", exp, got)
	}

	duration = rpi.ShardGroupDuration / 2
	if err := c.UpdateRetentionPolicy("db0", "rp0", &RetentionPolicyUpdate{
		Duration: &duration,
	}, true); err == nil {
		t.Fatal("expected error")
	} else if err != ErrIncompatibleDurations {
		t.Fatalf("expected error '%s', got '%s'", ErrIncompatibleDurations, err)
	}

	sgDuration := rpi.Duration * 2
	if err := c.UpdateRetentionPolicy("db0", "rp0", &RetentionPolicyUpdate{
		ShardGroupDuration: &sgDuration,
	}, true); err == nil {
		t.Fatal("expected error")
	} else if err != ErrIncompatibleDurations {
		t.Fatalf("expected error '%s', got '%s'", ErrIncompatibleDurations, err)
	}

	duration = rpi.ShardGroupDuration
	sgDuration = rpi.Duration
	if err := c.UpdateRetentionPolicy("db0", "rp0", &RetentionPolicyUpdate{
		Duration:           &duration,
		ShardGroupDuration: &sgDuration,
	}, true); err == nil {
		t.Fatal("expected error")
	} else if err != ErrIncompatibleDurations {
		t.Fatalf("expected error '%s', got '%s'", ErrIncompatibleDurations, err)
	}

	duration = time.Duration(0)
	sgDuration = 168 * time.Hour
	if err := c.UpdateRetentionPolicy("db0", "rp0", &RetentionPolicyUpdate{
		Duration:           &duration,
		ShardGroupDuration: &sgDuration,
	}, true); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestMetaClient_DropRetentionPolicy(t *testing.T) {
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

	duration := 1 * time.Hour
	replicaN := 1
	if _, err := c.CreateRetentionPolicy("db0", &RetentionPolicySpec{
		Name:     "rp0",
		Duration: &duration,
		ReplicaN: &replicaN,
	}, true); err != nil {
		t.Fatal(err)
	}

	rp, err := c.RetentionPolicy("db0", "rp0")
	if err != nil {
		t.Fatal(err)
	} else if rp.Name != "rp0" {
		t.Fatalf("rp name wrong: %s", rp.Name)
	} else if rp.Duration != time.Hour {
		t.Fatalf("rp duration wrong: %s", rp.Duration.String())
	} else if rp.ReplicaN != 1 {
		t.Fatalf("rp replication wrong: %d", rp.ReplicaN)
	}

	if err := c.DropRetentionPolicy("db0", "rp0"); err != nil {
		t.Fatal(err)
	}

	rp, err = c.RetentionPolicy("db0", "rp0")
	if err != nil {
		t.Fatal(err)
	} else if rp != nil {
		t.Fatalf("rp should have been dropped")
	}
}

func TestMetaClient_CreateUser(t *testing.T) {
	t.Parallel()

	dir, c := newClient()
	defer os.RemoveAll(dir)
	defer c.Close()

	if _, err := c.CreateUser("Jerry", "supersecure", true); err != nil {
		t.Fatal(err)
	}

	if _, err := c.CreateUser("Tom", "password", false); err != nil {
		t.Fatal(err)
	}

	u, err := c.User("Jerry")
	if err != nil {
		t.Fatal(err)
	}
	if exp, got := "Jerry", u.ID(); exp != got {
		t.Fatalf("unexpected user name: exp: %s got: %s", exp, got)
	}
	if !isAdmin(u) {
		t.Fatalf("expected user to be admin")
	}

	u, err = c.Authenticate("Jerry", "supersecure")
	if u == nil || err != nil || u.ID() != "Jerry" {
		t.Fatalf("failed to authenticate")
	}

	u, err = c.Authenticate("Jerry", "badpassword")
	if u != nil || err != ErrAuthenticate {
		t.Fatalf("authentication should fail with %s", ErrAuthenticate)
	}

	u, err = c.Authenticate("Jerry", "")
	if u != nil || err != ErrAuthenticate {
		t.Fatalf("authentication should fail with %s", ErrAuthenticate)
	}

	if err := c.UpdateUser("Jerry", "moresupersecure"); err != nil {
		t.Fatal(err)
	}

	u, err = c.Authenticate("Jerry", "supersecure")
	if u != nil || err != ErrAuthenticate {
		t.Fatalf("authentication should fail with %s", ErrAuthenticate)
	}

	u, err = c.Authenticate("Jerry", "moresupersecure")
	if u == nil || err != nil || u.ID() != "Jerry" {
		t.Fatalf("failed to authenticate")
	}

	u, err = c.Authenticate("foo", "")
	if u != nil || err != ErrUserNotFound {
		t.Fatalf("authentication should fail with %s", ErrUserNotFound)
	}

	u, err = c.User("Tom")
	if err != nil {
		t.Fatal(err)
	}
	if exp, got := "Tom", u.ID(); exp != got {
		t.Fatalf("unexpected user name: exp: %s got: %s", exp, got)
	}
	if isAdmin(u) {
		t.Fatalf("expected user not to be an admin")
	}

	if exp, got := 2, c.UserCount(); exp != got {
		t.Fatalf("unexpected user count.  got: %d exp: %d", got, exp)
	}

	if err := c.SetAdminPrivilege("Tom", true); err != nil {
		t.Fatal(err)
	}

	u, err = c.User("Tom")
	if err != nil {
		t.Fatal(err)
	}
	if exp, got := "Tom", u.ID(); exp != got {
		t.Fatalf("unexpected user name: exp: %s got: %s", exp, got)
	}
	if !isAdmin(u) {
		t.Fatalf("expected user to be an admin")
	}

	if err := c.SetAdminPrivilege("Tom", false); err != nil {
		t.Fatal(err)
	}

	u, err = c.User("Tom")
	if err != nil {
		t.Fatal(err)
	}
	if exp, got := "Tom", u.ID(); exp != got {
		t.Fatalf("unexpected user name: exp: %s got: %s", exp, got)
	}
	if isAdmin(u) {
		t.Fatalf("expected user not to be an admin")
	}

	if _, err := c.CreateDatabase("db0"); err != nil {
		t.Fatal(err)
	}

	db := c.Database("db0")
	if db.Name != "db0" {
		t.Fatalf("db name wrong: %s", db.Name)
	}

	if err := c.SetPrivilege("Tom", "db0", cnosql.ReadPrivilege); err != nil {
		t.Fatal(err)
	}

	p, err := c.UserPrivilege("Tom", "db0")
	if err != nil {
		t.Fatal(err)
	}
	if p == nil {
		t.Fatal("expected privilege but was nil")
	}
	if exp, got := cnosql.ReadPrivilege, *p; exp != got {
		t.Fatalf("unexpected privilege.  exp: %d, got: %d", exp, got)
	}

	if err := c.SetPrivilege("Tom", "db0", cnosql.NoPrivileges); err != nil {
		t.Fatal(err)
	}
	p, err = c.UserPrivilege("Tom", "db0")
	if err != nil {
		t.Fatal(err)
	}
	if p == nil {
		t.Fatal("expected privilege but was nil")
	}
	if exp, got := cnosql.NoPrivileges, *p; exp != got {
		t.Fatalf("unexpected privilege.  exp: %d, got: %d", exp, got)
	}

	if err := c.DropUser("Tom"); err != nil {
		t.Fatal(err)
	}

	if _, err = c.User("Tom"); err != ErrUserNotFound {
		t.Fatalf("user lookup should fail with %s", ErrUserNotFound)
	}

	if exp, got := 1, c.UserCount(); exp != got {
		t.Fatalf("unexpected user count.  got: %d exp: %d", got, exp)
	}
}

func TestMetaClient_UpdateUser_Exists(t *testing.T) {
	t.Parallel()

	dir, c := newClient()
	defer os.RemoveAll(dir)
	defer c.Close()

	if _, err := c.CreateUser("Jerry", "supersecure", true); err != nil {
		t.Fatal(err)
	}

	if err := c.UpdateUser("Jerry", "password"); err != nil {
		t.Fatal(err)
	}
}

func TestMetaClient_UpdateUser_NonExists(t *testing.T) {
	t.Parallel()

	dir, c := newClient()
	defer os.RemoveAll(dir)
	defer c.Close()

	if err := c.UpdateUser("foo", "bar"); err == nil {
		t.Fatalf("expected error, got nil")
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
