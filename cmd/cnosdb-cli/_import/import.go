package _import

import (
	"fmt"
	"net"
	"net/url"
	"strconv"

	"github.com/cnosdatabase/cnosdb/client"
	"github.com/cnosdatabase/cnosdb/pkg/importer"

	"github.com/spf13/cobra"
)

const (
	// defaultFormat is the default format of the results when issuing queries
	defaultFormat = "column"

	// defaultPrecision is the default timestamp format of the results when issuing queries
	defaultPrecision = "ns"

	// defaultPPS is the default points per second that the import will throttle at
	// by default it's 0, which means it will not throttle
	defaultPPS = 0
)

var (
	config = importer.NewConfig()
	env    = &options{}
)

type options struct {
	Host string
	Port int
	Ssl  bool
}

func GetCommand() *cobra.Command {
	c := &cobra.Command{
		Use:     "import [path] [Use]",
		Short:   "Import a previous database export from file. [Short]",
		Long:    description,
		Example: examples,
		CompletionOptions: cobra.CompletionOptions{
			DisableDefaultCmd:   true,
			DisableDescriptions: true,
			DisableNoDescFlag:   true,
		},
		Run: func(cmd *cobra.Command, args []string) {
			addr := net.JoinHostPort(env.Host, strconv.Itoa(env.Port))
			u, err := parseConnectionString(addr, env.Ssl)
			if err != nil {
				fmt.Printf("[ERR] %s\n", err)
				return
			}
			config.URL = u
			config.ClientConfig.Addr = u.String()

			i := importer.NewImporter(*config)
			if err := i.Import(); err != nil {
				fmt.Printf("[ERR] %s\n", err)
			}
		},
	}

	flags := c.Flags()
	flags.StringVar(&env.Host, "host", client.DEFAULT_HOST, "Host of the CnosDB instance to connect to.")
	flags.IntVar(&env.Port, "port", client.DEFAULT_PORT, "Port of the CnosDB instance to connect to.")
	flags.StringVarP(&config.ClientConfig.Username, "username", "u", "", "Username to login to the server.")
	flags.StringVarP(&config.ClientConfig.Password, "password", "p", "", `Password to login to the server. If password is not given, it's the same as using (--password="").`)
	flags.BoolVar(&env.Ssl, "ssl", false, "Use https for connecting to cluster.")

	flags.StringVar(&config.Precision, "precision", defaultPrecision, "Precision specifies the format of the timestamp:  rfc3339,h,m,s,ms,u or ns.")
	flags.StringVar(&config.WriteConsistency, "consistency", "all", "Set write consistency level: any, one, quorum, or all.")
	flags.StringVar(&config.Path, "path", "", "Path to the file to import.")
	flags.IntVar(&config.PPS, "pps", defaultPPS, "How many points per second the import will allow.  By default it is zero and will not throttle importing.")
	flags.BoolVar(&config.Compressed, "compressed", false, "set to true if the import file is compressed")
	return c
}

var description = `Import a previous database export from file. [Long]`

var examples = `Import a previous database export from file. [Example]`

// parseConnectionString 将 path 转换为合法 URL
func parseConnectionString(path string, ssl bool) (url.URL, error) {
	var host string
	var port int

	h, p, err := net.SplitHostPort(path)
	if err != nil {
		if path == "" {
			host = client.DEFAULT_HOST
		} else {
			host = path
		}
		port = client.DEFAULT_PORT
	} else {
		host = h
		port, err = strconv.Atoi(p)
		if err != nil {
			return url.URL{}, fmt.Errorf("invalid port number %q: %s\n", path, err)
		}
	}

	u := url.URL{
		Scheme: "http",
		Host:   host,
	}
	if ssl {
		u.Scheme = "https"
		if port != 443 {
			u.Host = net.JoinHostPort(host, strconv.Itoa(port))
		}
	} else if port != 80 {
		u.Host = net.JoinHostPort(host, strconv.Itoa(port))
	}

	return u, nil
}
