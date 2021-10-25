package _import

import (
	"github.com/cnosdatabase/cnosdb/pkg/importer"

	"github.com/pkg/errors"
	"github.com/spf13/cobra"
)

var (
	importConfig = importer.Config{}
)

func GetCommand() *cobra.Command {
	c := &cobra.Command{
		Use:     "import [path] [Use]",
		Short:   "Import a previous database export from file. [Short]",
		Long:    description,
		Example: examples,
		CompletionOptions: cobra.CompletionOptions{
			DisableDefaultCmd: true,
			DisableDescriptions: true,
			DisableNoDescFlag: true,
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			if parent := cmd.Parent(); parent != nil {
				parent.Run(cmd, args)
			}

			im := importer.NewImporter(importConfig)
			if err := im.Import(); err != nil {
				return errors.Unwrap(err)
			}
			return nil
		},
	}

	pFlags := c.PersistentFlags()
	pFlags.StringVar(&importConfig.Path, "path", "", "Path to the file to import.")
	return c
}

var description = `Import a previous database export from file. [Long]`

var examples = `Import a previous database export from file. [Example]`
