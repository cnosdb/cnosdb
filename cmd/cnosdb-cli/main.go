package main

import (
	"fmt"

	"github.com/cnosdb/cnosdb/cmd/cnosdb-cli/_import"
	"github.com/cnosdb/cnosdb/cmd/cnosdb-cli/cli"
	"github.com/cnosdb/cnosdb/cmd/cnosdb-tools/sql2cnosdb"

	"github.com/spf13/cobra"
)

var (
	version string
)

func init() {
	if version == "" {
		version = "unknown"
	}
}

func main() {
	cliCmd := cli.GetCommand(version)
	importCmd := _import.GetCommand()
	cliCmd.AddCommand(importCmd)
	printVersionCmd := printVersionCmd()
	cliCmd.AddCommand(printVersionCmd)
	cliCmd.AddCommand(sql2cnosdb.GetCommand())

	if err := cliCmd.Execute(); err != nil {
		fmt.Printf("Error : %+v\n", err)
	}
}

func printVersionCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Long:  "Display the version and exit",
		Short: "Display the version and exit",
		CompletionOptions: cobra.CompletionOptions{
			DisableDefaultCmd:   true,
			DisableNoDescFlag:   true,
			DisableDescriptions: true,
		},
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Printf("CnosDB shell version: v%s\n", version)
		},
	}
}
