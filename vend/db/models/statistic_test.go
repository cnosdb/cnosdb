package models_test

import (
	"github.com/cnosdb/cnosdb/vend/db/models"
	"reflect"
	"testing"
)

func TestTags_Merge(t *testing.T) {
	examples := []struct {
		Base   map[string]string
		Arg    map[string]string
		Result map[string]string
	}{
		{
			Base:   nil,
			Arg:    nil,
			Result: map[string]string{},
		},
		{
			Base:   nil,
			Arg:    map[string]string{"air": "air"},
			Result: map[string]string{"air": "air"},
		},
		{
			Base:   map[string]string{"air": "air"},
			Arg:    nil,
			Result: map[string]string{"air": "air"},
		},
		{
			Base:   map[string]string{"air": "air"},
			Arg:    map[string]string{"air": "air"},
			Result: map[string]string{"air": "air"},
		},
		{
			Base:   map[string]string{"air": "air"},
			Arg:    map[string]string{"bar": "bar"},
			Result: map[string]string{"air": "air", "bar": "bar"},
		},
		{
			Base:   map[string]string{"air": "air", "bar": "bar"},
			Arg:    map[string]string{"zoo": "zoo"},
			Result: map[string]string{"air": "air", "bar": "bar", "zoo": "zoo"},
		},
		{
			Base:   map[string]string{"air": "air", "bar": "bar"},
			Arg:    map[string]string{"bar": "newbar"},
			Result: map[string]string{"air": "air", "bar": "newbar"},
		},
	}

	for i, example := range examples {
		i++
		result := models.StatisticTags(example.Base).Merge(example.Arg)
		if got, exp := result, example.Result; !reflect.DeepEqual(got, exp) {
			t.Errorf("[Example %d] got %#v, expected %#v", i, got, exp)
		}
	}
}
