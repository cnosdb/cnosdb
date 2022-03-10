package main

import (
	"fmt"

	"github.com/cnosdb/cnosdb/cmd/cnosdb-cli/_import"
	"github.com/cnosdb/cnosdb/cmd/cnosdb-cli/cli"
	"github.com/cnosdb/cnosdb/cmd/cnosdb-cli/export"
)

func main() {
	cliCmd := cli.GetCommand()
	importCmd := _import.GetCommand()
	cliCmd.AddCommand(importCmd)
	exportCmd := export.GetCommand()
	cliCmd.AddCommand(exportCmd)

	if err := cliCmd.Execute(); err != nil {
		fmt.Printf("Error : %+v\n", err)
	}
}
