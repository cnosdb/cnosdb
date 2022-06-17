package query_test

import (
	"github.com/cnosdb/cnosdb/vend/db/pkg/deep"
	"github.com/cnosdb/cnosdb/vend/db/query"
	"reflect"
	"strings"
	"testing"
)

func TestPoint_Clone_Float(t *testing.T) {
	p := &query.FloatPoint{
		Name:  "air",
		Tags:  ParseTags("station=A"),
		Time:  5,
		Value: 2.1,
		Aux:   []interface{}{float64(54)},
	}
	c := p.Clone()
	if p == c {
		t.Errorf("clone failed, they have the same address")
	}
	if !deep.Equal(p, c) {
		t.Errorf("mismatched point")
	}
	if &p.Aux[0] == &c.Aux[0] {
		t.Errorf("Aux values have the same address.%v == %v", &p.Aux[0], &c.Aux[0])
	} else if !deep.Equal(p.Aux, c.Aux) {
		t.Errorf("mismatch in Aux fields")
	}

}

func TestPoint_Clone_Integer(t *testing.T) {
	p := &query.IntegerPoint{
		Name:  "air",
		Tags:  ParseTags("station=A"),
		Time:  5,
		Value: 2,
		Aux:   []interface{}{float64(54)},
	}
	c := p.Clone()
	if p == c {
		t.Errorf("clone failed, they have the same address")
	}
	if !deep.Equal(p, c) {
		t.Errorf("mismatched point")
	}
	if &p.Aux[0] == &c.Aux[0] {
		t.Errorf("Aux values have the same address.%v == %v", &p.Aux[0], &c.Aux[0])
	} else if !deep.Equal(p.Aux, c.Aux) {
		t.Errorf("mismatch in Aux fields")
	}

}

func TestPoint_Clone_String(t *testing.T) {
	p := &query.StringPoint{
		Name:  "air",
		Tags:  ParseTags("station=A"),
		Time:  5,
		Value: "test",
		Aux:   []interface{}{float64(54), "hello"},
	}
	c := p.Clone()
	if p == c {
		t.Errorf("clone failed, they have the same address")
	}
	if !deep.Equal(p, c) {
		t.Errorf("mismatched point")
	}
	if &p.Aux[0] == &c.Aux[0] {
		t.Errorf("Aux values have the same address.%v == %v", &p.Aux[0], &c.Aux[0])
	} else if !deep.Equal(p.Aux, c.Aux) {
		t.Errorf("mismatch in Aux fields")
	}

}

func TestPoint_Clone_Boolean(t *testing.T) {
	p := &query.BooleanPoint{
		Name:  "air",
		Tags:  ParseTags("station=A"),
		Time:  5,
		Value: true,
		Aux:   []interface{}{float64(54), "hello"},
	}
	c := p.Clone()
	if p == c {
		t.Errorf("clone failed, they have the same address")
	}
	if !deep.Equal(p, c) {
		t.Errorf("mismatched point")
	}
	if &p.Aux[0] == &c.Aux[0] {
		t.Errorf("Aux values have the same address.%v == %v", &p.Aux[0], &c.Aux[0])
	} else if !deep.Equal(p.Aux, c.Aux) {
		t.Errorf("mismatch in Aux fields")
	}

}

func TestPoint_Clone_Nil(t *testing.T) {
	var fp *query.FloatPoint
	if p := fp.Clone(); p != nil {
		t.Errorf("expected nil,got %v", p)
	}

	var ip *query.IntegerPoint
	if p := ip.Clone(); p != nil {
		t.Errorf("expected nil,got %v", p)
	}

	var sp *query.StringPoint
	if p := sp.Clone(); p != nil {
		t.Errorf("expected nil,got %v", p)
	}

	var bp *query.BooleanPoint
	if p := bp.Clone(); p != nil {
		t.Errorf("expected nil,got %v", p)
	}
}

func TestPoint_Fields(t *testing.T) {
	allowedFields := map[string]bool{
		"Name":       true,
		"Tags":       true,
		"Time":       true,
		"Nil":        true,
		"Value":      true,
		"Aux":        true,
		"Aggregated": true,
	}

	for _, typ := range []reflect.Type{
		reflect.TypeOf(query.FloatPoint{}),
		reflect.TypeOf(query.IntegerPoint{}),
		reflect.TypeOf(query.StringPoint{}),
		reflect.TypeOf(query.BooleanPoint{}),
	} {
		f, ok := typ.FieldByNameFunc(func(name string) bool {
			return !allowedFields[name]
		})
		if ok {
			t.Errorf("The field (%s: %s %s) is not allowed", typ, f.Name, f.Type)
		}
	}
}

func TestTags_ID(t *testing.T) {
	tags := query.NewTags(map[string]string{"air": "bar", "sea": "bat"})
	if id := tags.ID(); id != "air\x00sea\x00bar\x00bat" {
		t.Fatalf("unexpected ID, got: %s", id)
	}
}

func TestTags_Subset(t *testing.T) {
	tags := query.NewTags(map[string]string{"a": "0", "b": "1", "c": "2"})
	subset := tags.Subset([]string{"b", "c", "d"})
	//subset: map[b:1 c:2 d:]
	if keys := subset.Keys(); !reflect.DeepEqual(keys, []string{"b", "c", "d"}) {
		t.Fatalf("unexpected keys: %+v", keys)
	} else if v := subset.Value("a"); v != "" {
		t.Fatalf("unexpected 'a' value: %s", v)
	} else if v := subset.Value("b"); v != "1" {
		t.Fatalf("unexpected 'b' value: %s", v)
	} else if v := subset.Value("c"); v != "2" {
		t.Fatalf("unexpected 'c' value: %s", v)
	} else if v := subset.Value("d"); v != "" {
		t.Fatalf("unexpected 'd' value: %s", v)
	}
}

func TestTags_Equal(t *testing.T) {
	tag1 := query.NewTags(map[string]string{"a": "1", "b": "2"})
	tag2 := query.NewTags(map[string]string{"a": "1", "b": "2"})

	if !tag1.Equals(&tag2) {
		t.Fatalf("tag1 should equal to tag2")
	}
}

func ParseTags(s string) query.Tags {
	m := make(map[string]string)
	for _, kv := range strings.Split(s, ",") {
		a := strings.Split(kv, "=")
		m[a[0]] = a[1]
	}
	return query.NewTags(m)
}
