package verify

import (
	"github.com/spf13/cobra"
	"io"
	"os"
)

type Options struct {
	Stderr io.Writer
	Stdout io.Writer

	path string
}

func NewOptions() *Options {
	return &Options{
		Stderr: os.Stderr,
		Stdout: os.Stdout,
	}
}

var opt = NewOptions()

func GetCommand() *cobra.Command {
	return nil
}

func verify(cmd *cobra.Command, args []string) error {
	return nil
}
