package suite

import (
	"fmt"
	"testing"
)

func AssertEqual(t *testing.T, a, b interface{}) {
	if a != b {
		fmt.Print("A: ")
		fmt.Println(a)
		fmt.Print("B: ")
		fmt.Println(b)
		t.Error("A should be equal to B.")
	}
}

func AssertNotEqual(t *testing.T, a, b interface{}) {
	if a == b {
		fmt.Print("A: ")
		fmt.Println(a)
		fmt.Print("B: ")
		fmt.Println(b)
		t.Error("A should not be equal to B.")
	}
}
