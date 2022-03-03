package verify

import (
	"github.com/spf13/cobra"
)

type Options struct {
}

func NewOptions() *Options {
	return nil
}

var opt = NewOptions()

func GetCommand() *cobra.Command {
	return nil
}

func verify(cmd *cobra.Command, args []string) error {
	return nil
}
