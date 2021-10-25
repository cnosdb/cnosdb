package cli

import (
	"fmt"
	"os"
	"strings"

	"github.com/cnosdatabase/cnosdb/client"

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
		Short:   "cnosdb-cli [Short]",
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

	pFlags := c.PersistentFlags()
	pFlags.StringVar(&commandLine.Host, "host", client.DEFAULT_HOST, "Host of the CnosDB instance to connect to.")
	pFlags.IntVar(&commandLine.Port, "port", client.DEFAULT_PORT, "Port of the CnosDB instance to connect to.")
	pFlags.StringVarP(&commandLine.clientConfig.Username, "username", "u", "", "Username to login to the server.")
	pFlags.StringVarP(&commandLine.clientConfig.Password, "password", "p", "", `Password to login to the server. If password is not given, it's the same as using (--password="").`)
	pFlags.BoolVar(&commandLine.Ssl, "ssl", false, "Use https for connecting to cluster.")

	lFlags := c.LocalFlags()
	lFlags.StringVar(&commandLine.Format, "format", defaultFormat, "The format of the server responses:  json, csv, or column.")
	lFlags.StringVar(&commandLine.pointConfig.Precision, "precision", defaultPrecision, "The format of the timestamp:  rfc3339,h,m,s,ms,u or ns.")

	return c
}

var description = `The Kubernetes API server validates and configures data
for the api objects which include pods, services, replicationcontrollers, and
others. The API Server services REST operations and provides the frontend to the
cluster's shared state through which all other components interact.`

var examples = `$ cnos
$ cnos -format=json -pretty
$ cnos -execute 'SHOW DATABASES'
$ cnos -execute 'SELECT * FROM "h2o_feet" LIMIT 3' -database="NOAA_water_database" -precision=rfc3339
$ cnos -import -path=data.txt -precision=s`
