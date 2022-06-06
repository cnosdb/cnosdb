package monitor

import (
	"reflect"
	"testing"
)

func TestDiagnostics_BuildInfo(t *testing.T) {
	s := New(nil, Config{})
	s.Version = "1.1.0"
	s.Commit = "0c983b993e465c6380f78cb25ccb184ca91e194b"
	s.Branch = "1.1"
	s.BuildTime = "8m15s"

	if err := s.Open(); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	defer s.Close()

	d, err := s.Diagnostics()
	if err != nil {
		t.Errorf("unexpected error: %s", err)
		return
	}

	diags, ok := d["build"]
	if !ok {
		t.Error("no diagnostics found for 'build'")
		return
	}

	if got, exp := diags.Columns, []string{"Branch", "Build Time", "Commit", "Version"}; !reflect.DeepEqual(got, exp) {
		t.Errorf("unexpected columns: got=%v exp=%v", got, exp)
	}

	if got, exp := diags.Rows, [][]interface{}{
		{"1.1", "8m15s", "0c983b993e465c6380f78cb25ccb184ca91e194b", "1.1.0"},
	}; !reflect.DeepEqual(got, exp) {
		t.Errorf("unexpected rows: got=%v exp=%v", got, exp)
	}
}
