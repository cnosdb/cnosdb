package main

import (
	"fmt"

	"github.com/cnosdb/cnosdb/cmd/cnosdb-inspect/buildtsi"
	"github.com/cnosdb/cnosdb/cmd/cnosdb-inspect/deletetsm"
	"github.com/cnosdb/cnosdb/cmd/cnosdb-inspect/dumptsi"
	"github.com/cnosdb/cnosdb/cmd/cnosdb-inspect/dumptsm"
	"github.com/cnosdb/cnosdb/cmd/cnosdb-inspect/dumptsmwal"
	"github.com/cnosdb/cnosdb/cmd/cnosdb-inspect/export"
	"github.com/cnosdb/cnosdb/cmd/cnosdb-inspect/report"
	"github.com/cnosdb/cnosdb/cmd/cnosdb-inspect/reportdisk"
	"github.com/cnosdb/cnosdb/cmd/cnosdb-inspect/reporttsi"
	"github.com/cnosdb/cnosdb/cmd/cnosdb-inspect/verify/seriesfile"
	"github.com/cnosdb/cnosdb/cmd/cnosdb-inspect/verify/tsm"
	"github.com/cnosdb/cnosdb/cmd/cnosdb-inspect/verify/verify-tombstone"

	"github.com/spf13/cobra"
)

func main() {

	mainCmd := GetCommand()

	verifyCmd := verify.GetCommand()
	mainCmd.AddCommand(verifyCmd)

	verifytombstoneCmd := verifytombstone.GetCommand()
	mainCmd.AddCommand(verifytombstoneCmd)

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
		Use:  "cnosdb-inspect",
		Long: "cnosdb-inspect Inspect is an CnosDB disk utility",
		CompletionOptions: cobra.CompletionOptions{
			DisableDefaultCmd:   true,
			DisableNoDescFlag:   true,
			DisableDescriptions: true},
	}

	return c
}
