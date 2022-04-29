package main

import (
	"fmt"
	"math/rand"
	"os"
	"time"

	"github.com/cnosdb/cnosdb/cmd/cnosdb/backup"
	"github.com/cnosdb/cnosdb/cmd/cnosdb/options"
	"github.com/cnosdb/cnosdb/cmd/cnosdb/restore"
	"github.com/cnosdb/cnosdb/cmd/cnosdb/run"

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

var cnosdb_examples = `  cnosdb
  cnosdb --config ./cnosdb.config`

func main() {
	rand.Seed(time.Now().UnixNano())
	mainCmd := GetCommand()
	setFlags(mainCmd)
	runCmd := run.GetCommand()
	setFlags(runCmd)
	mainCmd.AddCommand(runCmd)
	configCmd := run.GetConfigCommand()
	mainCmd.AddCommand(configCmd)
	backupCmd := backup.GetCommand()
	mainCmd.AddCommand(backupCmd)
	restoreCmd := restore.GetCommand()
	mainCmd.AddCommand(restoreCmd)
	buildInfoCmd := printBuildInfo()
	mainCmd.AddCommand(buildInfoCmd)

	if err := mainCmd.Execute(); err != nil {
		fmt.Printf("Error : %+v\n", err)
	}
}

func GetCommand() *cobra.Command {
	c := &cobra.Command{
		Use:     "cnosdb [command]",
		Long:    "The 'cnosdb' command starts and runs all the processes necessary for CnosDB to function.",
		Example: cnosdb_examples,
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

func printBuildInfo() *cobra.Command {
	return &cobra.Command{
		Use:  "version",
		Long: "displays the CnosDB version",
		CompletionOptions: cobra.CompletionOptions{
			DisableDefaultCmd:   true,
			DisableNoDescFlag:   true,
			DisableDescriptions: true,
		},
		// example: CnosDB v0.10.0 (git: main c2b889e3)
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Printf("CnosDB v%s (git: %s %s)\n", version, branch, commit)
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
