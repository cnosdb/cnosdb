// The cnosdb_tools command displays detailed information about CnosDB data files.
package main

import (
	"fmt"

	"github.com/cnosdb/cnosdb/cmd/cnosdb-tools/compact"
	"github.com/cnosdb/cnosdb/cmd/cnosdb-tools/export"
	genExec "github.com/cnosdb/cnosdb/cmd/cnosdb-tools/generate/exec"
	genInit "github.com/cnosdb/cnosdb/cmd/cnosdb-tools/generate/init"
	"github.com/cnosdb/cnosdb/cmd/cnosdb-tools/importer"

	"github.com/spf13/cobra"
)

func main() {

	mainCmd := GetCommand()

	geninit := genInit.GetCommand()
	mainCmd.AddCommand(geninit)

	genexec := genExec.GetCommand()
	mainCmd.AddCommand(genexec)

	compact := compact.GetCommand()
	mainCmd.AddCommand(compact)

	importer := importer.GetCommand()
	mainCmd.AddCommand(importer)

	export := export.GetCommand()
	mainCmd.AddCommand(export)

	if err := mainCmd.Execute(); err != nil {
		fmt.Printf("Error : %+v\n", err)
	}

}

func GetCommand() *cobra.Command {
	c := &cobra.Command{
		Use:  "cnosdb-tools",
		Long: "tools for managing and querying CnosDB data",
		CompletionOptions: cobra.CompletionOptions{
			DisableDefaultCmd:   true,
			DisableNoDescFlag:   true,
			DisableDescriptions: true},
	}

	return c
}