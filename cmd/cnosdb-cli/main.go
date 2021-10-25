package main

import (
	"fmt"

	"github.com/cnosdatabase/cnosdb/cmd/cnosdb-cli/_import"
	"github.com/cnosdatabase/cnosdb/cmd/cnosdb-cli/cli"
)

func main() {
	cliCmd := cli.GetCommand()
	importCmd := _import.GetCommand()
	cliCmd.AddCommand(importCmd)

	if err := cliCmd.Execute(); err != nil {
		fmt.Printf("Error : %+v\n", err)
	}
}
