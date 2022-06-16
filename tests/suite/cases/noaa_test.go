package cases

import (
	"github.com/cnosdb/cnosdb/tests/suite/noaa"
	"testing"
)

func TestNoaa(t *testing.T) {
	n := noaa.NOAA{S: server, T: t}
	n.Load()
}
