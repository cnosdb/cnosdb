package main

import (
	"fmt"
	"math/rand"
	"os"
	"time"

	"github.com/cnosdb/cnosdb/cmd/cnosdb-meta/options"
	"github.com/cnosdb/cnosdb/cmd/cnosdb-meta/run"

	"github.com/spf13/cobra"
)

// These variables are populated via the Go linker.
var (
	version string
	commit  string
	branch  string
)

func init() {
	// If commit, branch, or build time are not set, make that clear.
	if version == "" {
		version = "unknown"
	}
	if commit == "" {
		commit = "unknown"
	}
	if branch == "" {
		branch = "unknown"
	}
}

var cnosdb_meta_examples = `  cnosdb-meta
  cnosdb-meta --config ./cnosdb.config`

func main() {
	rand.Seed(time.Now().UnixNano())
	mainCmd := GetCommand()
	setFlags(mainCmd)
	runCmd := run.GetCommand()
	setFlags(runCmd)
	mainCmd.AddCommand(runCmd)
	configCmd := run.GetConfigCommand()
	mainCmd.AddCommand(configCmd)
	mainCmd.AddCommand(printVersion())

	if err := mainCmd.Execute(); err != nil {
		fmt.Printf("Error : %+v\n", err)
	}
}

func GetCommand() *cobra.Command {
	c := &cobra.Command{
		Use:     "cnosdb-meta [command]",
		Long:    "The 'cnosdb-meta' command starts the CnosDB meta node.",
		Example: cnosdb_meta_examples,
		CompletionOptions: cobra.CompletionOptions{
			DisableDefaultCmd:   true,
			DisableDescriptions: true,
			DisableNoDescFlag:   true,
		},
		Run: func(cmd *cobra.Command, args []string) {
			runCmd := run.GetCommand()
			setFlags(runCmd)
			runArgs := os.Args[:]
			runArgs[0] = "run"
			runCmd.SetArgs(runArgs)
			runCmd.Flags()
			if err := runCmd.Execute(); err != nil {
				fmt.Printf("Error : %+v\n", err)
			}
		},
	}
	return c
}

func printVersion() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Displays the CnosDB meta version",
		CompletionOptions: cobra.CompletionOptions{
			DisableDefaultCmd:   true,
			DisableNoDescFlag:   true,
			DisableDescriptions: true,
		},
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Printf("CnosDB Meta v%s (git: %s %s)\n", version, branch, commit)
		},
	}
}

func setFlags(c *cobra.Command) {
	c.Flags().StringVarP(&options.Env.ConfigFile, "config", "c", "", `Set the path to the configuration file.
This defaults to the environment variable CNOSDB_CONFIG_PATH,
~/.cnosdb/cnosdb.conf, or /etc/cnosdb/cnosdb.conf if a file
is present at any of these locations.
Disable the automatic loading of a configuration file using
the null device (such as /dev/null)`)
	c.Flags().StringVarP(&options.Env.PidFile, "pidfile", "", "", "Write process ID to a file.")
	c.Flags().StringVarP(&options.Env.CpuProfile, "cpuprofile", "", "", "Write CPU profiling information to a file.")
	c.Flags().StringVarP(&options.Env.MemProfile, "memprofile", "", "", "Write memory usage information to a file.")
}
