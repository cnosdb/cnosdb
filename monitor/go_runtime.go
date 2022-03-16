package monitor

import (
	"github.com/cnosdb/cnosdb/vend/common/monitor/diagnostics"
	"runtime"
)

// goRuntime captures Go runtime diagnostics.
type goRuntime struct{}

func (g *goRuntime) Diagnostics() (*diagnostics.Diagnostics, error) {
	d := map[string]interface{}{
		"GOARCH":     runtime.GOARCH,
		"GOOS":       runtime.GOOS,
		"GOMAXPROCS": runtime.GOMAXPROCS(-1),
		"version":    runtime.Version(),
	}

	return diagnostics.RowFromMap(d), nil
}
