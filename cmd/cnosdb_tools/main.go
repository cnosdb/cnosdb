package main

import (
	"fmt"

	geninit "github.com/cnosdb/cnosdb/cmd/cnosdb_tools/generate/init"
	"github.com/spf13/cobra"
)

func main() {

	mainCmd := GetCommand()

	geninit := geninit.GetCommand()
	mainCmd.AddCommand(geninit)

	if err := mainCmd.Execute(); err != nil {
		fmt.Printf("Error : %+v\n", err)
	}

}

func GetCommand() *cobra.Command {
	c := &cobra.Command{
		Use:  "cnosdb_tools",
		Long: "tools for managing and querying CnosDB data",
		CompletionOptions: cobra.CompletionOptions{
			DisableDefaultCmd:   true,
			DisableNoDescFlag:   true,
			DisableDescriptions: true},
	}

	return c
}
