package main

import (
	"fmt"

	"github.com/cnosdb/cnosdb/cmd/cnosdb-ctl/node"
	"github.com/cnosdb/cnosdb/cmd/cnosdb-ctl/options"
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
	mainCmd := GetCommand()
	mainCmd.AddCommand(node.GetShowCommand())
	mainCmd.AddCommand(node.GetAddMetaCommand())
	mainCmd.AddCommand(node.GetRemoveMetaCommand())
	mainCmd.AddCommand(node.GetAddDataCommand())
	mainCmd.AddCommand(node.GetRemoveDataCommand())
	mainCmd.AddCommand(printVersion())

	if err := mainCmd.Execute(); err != nil {
		fmt.Printf("Error : %+v\n", err)
	}
}

func printVersion() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Displays the CnosDB meta version",
		CompletionOptions: cobra.CompletionOptions{
			DisableDefaultCmd:   true,
			DisableNoDescFlag:   true,
			DisableDescriptions: true,
		},
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Printf("cnosdb-cli: v%s \n", version)
		},
	}
}

func GetCommand() *cobra.Command {
	c := &cobra.Command{
		Use:  "cnosdb-ctl",
		Long: "The 'cnosdb-ctl' command is used for managing CnosDB clusters.",
		CompletionOptions: cobra.CompletionOptions{
			DisableDefaultCmd:   true,
			DisableDescriptions: true,
			DisableNoDescFlag:   true,
		},
	}

	c.PersistentFlags().StringVar(&options.Env.Bind, "bind", "127.0.0.1:8091", "")

	return c
}
