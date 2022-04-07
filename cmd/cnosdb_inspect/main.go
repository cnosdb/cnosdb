package main

import (
	"fmt"
	
	"github.com/cnosdb/cnosdb/cmd/cnosdb_inspect/buildtsi"
	"github.com/cnosdb/cnosdb/cmd/cnosdb_inspect/deletetsm"
	"github.com/cnosdb/cnosdb/cmd/cnosdb_inspect/dumptsi"
	"github.com/cnosdb/cnosdb/cmd/cnosdb_inspect/dumptsm"
	"github.com/cnosdb/cnosdb/cmd/cnosdb_inspect/dumptsmwal"
	"github.com/cnosdb/cnosdb/cmd/cnosdb_inspect/export"
	"github.com/cnosdb/cnosdb/cmd/cnosdb_inspect/report"
	"github.com/cnosdb/cnosdb/cmd/cnosdb_inspect/reportdisk"
	"github.com/cnosdb/cnosdb/cmd/cnosdb_inspect/reporttsi"
	"github.com/cnosdb/cnosdb/cmd/cnosdb_inspect/verify/seriesfile"
	"github.com/cnosdb/cnosdb/cmd/cnosdb_inspect/verify/tsm"
	
	"github.com/spf13/cobra"
)

func main() {

	mainCmd := GetCommand()

	verifyCmd := verify.GetCommand()
	mainCmd.AddCommand(verifyCmd)

	verifySeriesfileCmd := seriesfile.GetCommand()
	mainCmd.AddCommand(verifySeriesfileCmd)

	dumptsmCmd := dumptsm.GetCommand()
	mainCmd.AddCommand(dumptsmCmd)

	dumptsiCmd := dumptsi.GetCommand()
	mainCmd.AddCommand(dumptsiCmd)

	buildtsiCmd := buildtsi.GetCommand()
	mainCmd.AddCommand(buildtsiCmd)

	dumptsmwalCmd := dumptsmwal.GetCommand()
	mainCmd.AddCommand(dumptsmwalCmd)

	deletetsmCmd := deletetsm.GetCommand()
	mainCmd.AddCommand(deletetsmCmd)

	reportDiakCmd := reportdisk.GetCommand()
	mainCmd.AddCommand(reportDiakCmd)

	reportCmd := report.GetCommand()
	mainCmd.AddCommand(reportCmd)

	reporttsiCmd := reporttsi.GetCommand()
	mainCmd.AddCommand(reporttsiCmd)

	exportCmd := export.GetCommand()
	mainCmd.AddCommand(exportCmd)

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
