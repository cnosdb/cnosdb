package suite

import "github.com/cnosdb/cnosdb/tests"

type Generator interface {
	Run(server tests.Server)
}
