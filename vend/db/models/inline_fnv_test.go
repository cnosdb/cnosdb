package models_test

import (
	"github.com/cnosdb/cnosdb/vend/db/models"
	"hash/fnv"
	"testing"
	"testing/quick"
)

func TestInlineFNV64aEquivalenceFuzzy(t *testing.T) {
	f := func(data []byte) bool {
		stdlibFNV := fnv.New64a()
		_, err := stdlibFNV.Write(data)
		if err != nil {
			return false
		}
		want := stdlibFNV.Sum64()

		inlineFNV := models.NewInlineFNV64a()
		_, err = inlineFNV.Write(data)
		if err != nil {
			return false
		}
		got := inlineFNV.Sum64()

		return want == got

	}

	cfg := &quick.Config{
		MaxCount: 10000,
	}
	if err := quick.Check(f, cfg); err != nil {
		t.Fatal(err)
	}
}
