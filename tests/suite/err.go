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
