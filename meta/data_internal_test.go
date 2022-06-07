package meta

import (
	"fmt"
	"github.com/cnosdb/cnosdb/vend/db/pkg/testing/assert"
	"reflect"
	"sort"
	"testing"
	"time"
)

func TestShardGroupSort_Shard_EndTimeNotEqual(t *testing.T) {
	sg1 := ShardGroupInfo{
		ID:          1,
		StartTime:   time.Unix(1000, 0),
		EndTime:     time.Unix(1100, 0),
		TruncatedAt: time.Unix(1050, 0),
	}

	sg2 := ShardGroupInfo{
		ID:        2,
		StartTime: time.Unix(1000, 0),
		EndTime:   time.Unix(1100, 0),
	}

	sgs := ShardGroupInfos{sg2, sg1}

	sort.Sort(sgs)

	if sgs[len(sgs)-1].ID != 2 {
		t.Fatal("unstable sort for ShardGroupInfos")
	}
}

func TestShardGroupSort_Shard_EndTimeEqual(t *testing.T) {
	sg1 := ShardGroupInfo{
		ID:        1,
		StartTime: time.Unix(900, 0),
		EndTime:   time.Unix(1100, 0),
	}

	sg2 := ShardGroupInfo{
		ID:        2,
		StartTime: time.Unix(1000, 0),
		EndTime:   time.Unix(1100, 0),
	}

	sgs := ShardGroupInfos{sg2, sg1}

	sort.Sort(sgs)

	if sgs[len(sgs)-1].ID != 2 {
		t.Fatal("unstable sort for ShardGroupInfos")
	}
}

func TestShardGroupInfo_Contains(t *testing.T) {
	sgi := &ShardGroupInfo{StartTime: time.Unix(10, 0), EndTime: time.Unix(20, 0)}

	tests := []struct {
		ts  time.Time
		exp bool
	}{
		{time.Unix(0, 0), false},
		{time.Unix(9, 0), false},
		{time.Unix(10, 0), true},
		{time.Unix(11, 0), true},
		{time.Unix(15, 0), true},
		{time.Unix(19, 0), true},
		{time.Unix(20, 0), false},
		{time.Unix(21, 0), false},
	}
	for _, test := range tests {
		t.Run(fmt.Sprintf("ts=%d", test.ts.Unix()), func(t *testing.T) {
			got := sgi.Contains(test.ts)
			assert.Equal(t, got, test.exp)
		})
	}
}

func Test_Data_RetentionPolicy_MarshalBinary(t *testing.T) {
	zeroTime := time.Time{}
	epoch := time.Unix(0, 0).UTC()

	startTime := zeroTime
	sgi := &ShardGroupInfo{
		StartTime: startTime,
	}
	isgi := sgi.marshal()
	sgi.unmarshal(isgi)
	if got, exp := sgi.StartTime.UTC(), epoch.UTC(); got != exp {
		t.Errorf("unexpected start time.  got: %s, exp: %s", got, exp)
	}

	startTime = time.Unix(0, 0)
	endTime := startTime.Add(time.Hour * 24)
	sgi = &ShardGroupInfo{
		StartTime: startTime,
		EndTime:   endTime,
	}
	isgi = sgi.marshal()
	sgi.unmarshal(isgi)
	if got, exp := sgi.StartTime.UTC(), startTime.UTC(); got != exp {
		t.Errorf("unexpected start time.  got: %s, exp: %s", got, exp)
	}
	if got, exp := sgi.EndTime.UTC(), endTime.UTC(); got != exp {
		t.Errorf("unexpected end time.  got: %s, exp: %s", got, exp)
	}
	if got, exp := sgi.DeletedAt.UTC(), zeroTime.UTC(); got != exp {
		t.Errorf("unexpected DeletedAt time.  got: %s, exp: %s", got, exp)
	}
}

func TestNodeInfo_serializes(t *testing.T) {

	node1 := &NodeInfo{1, "localhost", "127.0.0.1"}

	info := node1.marshal()
	if info == nil {
		t.Fatalf("marshal failed")
	}
	node2 := &NodeInfo{}
	node2.unmarshal(info)
	if !reflect.DeepEqual(node1, node2) {
		t.Fatalf("unmarshal failed")
	}
}
