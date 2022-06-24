package suite

import "testing"

func PanicErr(err error) {
	if err != nil {
		panic(err)
	}
}

func TestErr(t *testing.T, err error) {
	if err != nil {
		t.Error(err)
	}
}

func BenchErr(b *testing.B, err error) {
	if err != nil {
		b.Error(err)
	}
}
