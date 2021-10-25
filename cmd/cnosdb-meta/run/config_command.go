package run

import (
	"fmt"
	"os"

	"github.com/cnosdatabase/cnosdb/meta"
	"github.com/cnosdatabase/cnosdb/pkg/logger"
	"github.com/BurntSushi/toml"
	"github.com/spf13/cobra"
)

var config_examples = `  cnosdb-meta config`

func GetConfigCommand() *cobra.Command {
	c := &cobra.Command{
		Use:     "config",
		Short:   "display the default configuration",
		Long:    "Displays the default configuration.",
		Example: config_examples,
		CompletionOptions: cobra.CompletionOptions{
			DisableDefaultCmd:   true,
			DisableDescriptions: true,
			DisableNoDescFlag:   true,
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			var path string
			if c := cmd.Flag("config"); c != nil {
				path = c.Value.String()
			}

			c, err := meta.NewDemoConfig()
			if err != nil {
				c = meta.NewConfig()
			}
			c.HTTPD = meta.NewServerConfig()
			c.Log = logger.NewDefaultLogConfig()

			if path != "" {
				fmt.Fprintf(os.Stderr, "Merging with configuration at: %s\n", path)

				if err := c.FromTomlFile(path); err != nil {
					return err
				}

				if err := c.Validate(); err != nil {
					return fmt.Errorf("%s. To generate a valid configuration file run `cnosdb config > cnosdb.generated.conf`", err)
				}
			}

			toml.NewEncoder(os.Stdout).Encode(c)

			return nil
		},
	}

	c.Flags().StringP("config", "c", "", `Set the path to the configuration file.
This defaults to the environment variable CNOSDB_CONFIG_PATH,
~/.cnosdb/cnosdb.conf, or /etc/cnosdb/cnosdb.conf if a file
is present at any of these locations.
Disable the automatic loading of a configuration file using
the null device (such as /dev/null)`)

	return c
}
