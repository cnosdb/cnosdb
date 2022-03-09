package cli

import (
	"fmt"
	"os"
	"strings"

	"github.com/cnosdb/cnosdb/client"

	"github.com/spf13/cobra"
)

var (
	version string

	commandLine = newCommandLine(version)

	// promptForPassword
	promptForPassword = false
)

const (
	// defaultFormat 结果集的默认样式
	defaultFormat = "column"

	// defaultPrecision 结果集中时间戳的默认样式
	defaultPrecision = "ns"

	// defaultPPS 导入操作时的节流操作，用来限制每秒导入的 Point 数量。
	// 默认为 0 ，意为不进行节流
	defaultPPS = 0
)

func init() {
	if version == "" {
		version = "0.0.1"
	}
}

func GetCommand() *cobra.Command {
	c := &cobra.Command{
		Use:     "cnosdb-cli",
		Long:    description,
		Example: examples,
		CompletionOptions: cobra.CompletionOptions{
			DisableDefaultCmd:   true,
			DisableDescriptions: true,
			DisableNoDescFlag:   true,
		},
		PreRun: func(cmd *cobra.Command, args []string) {
			if commandLine.clientConfig.Password == "" {
				for _, arg := range args {
					a := strings.ToLower(arg)
					if strings.HasPrefix(a, "--password") || strings.HasPrefix(a, "-p") {
						promptForPassword = true
						break
					}
				}
			}
		},
		Run: func(cmd *cobra.Command, args []string) {
			if err := commandLine.Run(); err != nil {
				fmt.Fprintf(os.Stderr, "%s\n", err)
				os.Exit(1)
			}
		},
	}

	flags := c.Flags()
	flags.StringVar(&commandLine.Host, "host", client.DEFAULT_HOST, "Host of the CnosDB instance to connect to.")
	flags.IntVar(&commandLine.Port, "port", client.DEFAULT_PORT, "Port of the CnosDB instance to connect to.")
	flags.StringVarP(&commandLine.clientConfig.Username, "username", "u", "", "Username to login to the server.")
	flags.StringVarP(&commandLine.clientConfig.Password, "password", "p", "", `Password to login to the server. If password is not given, it's the same as using (--password="").`)
	flags.BoolVar(&commandLine.Ssl, "ssl", false, "Use https for connecting to cluster.")
	flags.StringVar(&commandLine.Format, "format", defaultFormat, "The format of the server responses:  json, csv, or column.")
	flags.BoolVar(&commandLine.Pretty, "pretty", false, "Turns on pretty print for the json format.")
	flags.StringVar(&commandLine.pointConfig.Precision, "precision", defaultPrecision, "The format of the timestamp:  rfc3339,h,m,s,ms,u or ns.")

	return c
}

var description = `CnosDB shell`

var examples = `  cnosdb-cli
  cnosdb-cli --format=json --pretty
  cnosdb-cli import --path dba-export.txt
  cnosdb-cli export --database dba --out dba-export.txt`
