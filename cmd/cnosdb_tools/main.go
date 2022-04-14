// The cnosdb_tools command displays detailed information about CnosDB data files.
package main

import (
	"fmt"

	_ "github.com/cnosdb/cnosdb/cmd/cnosdb/run"
	_ "github.com/cnosdb/cnosdb/cmd/cnosdb_tools/compact"
	_ "github.com/cnosdb/cnosdb/cmd/cnosdb_tools/export"
	_ "github.com/cnosdb/cnosdb/cmd/cnosdb_tools/generate/exec"
	_ "github.com/cnosdb/cnosdb/cmd/cnosdb_tools/generate/init"
	_ "github.com/cnosdb/cnosdb/cmd/cnosdb_tools/help"
	_ "github.com/cnosdb/cnosdb/cmd/cnosdb_tools/importer"
	_ "github.com/cnosdb/cnosdb/cmd/cnosdb_tools/server"
	_ "github.com/cnosdb/cnosdb/meta"
	_ "github.com/cnosdb/cnosdb/vend/db/tsdb"
	_ "github.com/cnosdb/cnosdb/vend/db/tsdb/engine"

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
