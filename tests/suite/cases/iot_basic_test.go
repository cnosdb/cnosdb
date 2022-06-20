package cases

import (
	"github.com/cnosdb/cnosdb/tests/suite/iot"
	"testing"
)

func TestIotBasicWrite(t *testing.T) {
	iot.Basic.Run(server, t)
}

func TestIotResCode(t *testing.T) {
	iot.Basic.ResCode(server)
}
