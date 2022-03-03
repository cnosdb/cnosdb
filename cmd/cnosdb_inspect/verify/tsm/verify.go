package verify

import (
	"fmt"
	"github.com/spf13/cobra"
	"io"
	"os"
	"os/user"
	"path"
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
	c := &cobra.Command{
		Use:   "verify",
		Short: "Verifies the integrity of TSM files.",
		Run: func(cmd *cobra.Command, args []string) {
			if err := verify(cmd, args); err != nil {
				fmt.Println(err)
			}
		},
	}
	var defaultDir string
	u, err := user.Current()
	if err == nil {
		defaultDir = path.Join(u.HomeDir, ".cnosdb")
	} else if os.Getenv("HOME") != "" {
		defaultDir = path.Join(os.Getenv("HOME"), ".cnosdb")
	} else {
		defaultDir = ""
	}
	c.PersistentFlags().StringVar(&opt.path, "dir", defaultDir, "The path to the storage root directory.")
	return c
}

func verify(cmd *cobra.Command, args []string) error {
	return nil
}
