package models

import (
	"fmt"
	"testing"
)

func TestMarshalPointNoFields(t *testing.T) {
	points, err := ParsePointsString("m,k=v f=0i")
	if err != nil {
		t.Fatal(err)
	}
	t.Log(points)

	// It's unclear how this can ever happen, but we've observed points that were marshalled without any fields.
	points[0].(*point).fields = []byte{}

	if _, err := points[0].MarshalBinary(); err != ErrPointMustHaveAField {
		t.Fatalf("got error %v, exp %v", err, ErrPointMustHaveAField)
	}
}

func TestInvalidPoint(t *testing.T) {
	_, err := ParsePointsString("m,k=v f=0i 321678749878973216 !")
	expErr := fmt.Errorf("unable to parse 'm,k=v f=0i 321678749878973216 !': point is invalid")
	if err.Error() != expErr.Error() {
		t.Fatal(err)
	}
}

func TestInvalidNumber(t *testing.T) {
	_, err := ParsePointsString("m,k=v f=0!i")
	expErr := fmt.Errorf("unable to parse 'm,k=v f=0!i': invalid number")
	if err.Error() != expErr.Error() {
		t.Fatal(err)
	}
}
