package monitor

import (
	"github.com/cnosdb/cnosdb/vend/common/monitor/diagnostics"
	"os"
)

// network captures network diagnostics.
type network struct{}

func (n *network) Diagnostics() (*diagnostics.Diagnostics, error) {
	h, err := os.Hostname()
	if err != nil {
		return nil, err
	}

	d := map[string]interface{}{
		"hostname": h,
	}

	return diagnostics.RowFromMap(d), nil
}
