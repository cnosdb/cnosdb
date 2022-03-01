package main

import (
	"fmt"

	"github.com/cnosdb/cnosdb/cmd/cnosdb_inspect/dumptsm"
	"github.com/spf13/cobra"
)

func main() {

	mainCmd := GetCommand()

	dumptsmCmd := dumptsm.GetCommand()
	mainCmd.AddCommand(dumptsmCmd)

	if err := mainCmd.Execute(); err != nil {
		fmt.Printf("Error : %+v\n", err)
	}

}

func GetCommand() *cobra.Command {
	c := &cobra.Command{
		Use:  "cnosdb_inspect",
		Long: "cnosdb_inspect Inspect is an CnosDB disk utility",
		CompletionOptions: cobra.CompletionOptions{
			DisableDefaultCmd:   true,
			DisableNoDescFlag:   true,
			DisableDescriptions: true},
	}

	return c
}
