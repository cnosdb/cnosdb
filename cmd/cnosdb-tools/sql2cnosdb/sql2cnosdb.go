package sql2cnosdb

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/cnosdb/cnosdb/client"
	"github.com/cnosdb/cnosdb/pkg/importer"
	"github.com/spf13/cobra"
)

var (
	config = importer.NewConfig()
	env    = &options{}
)

var useExp *regexp.Regexp
var connectExp *regexp.Regexp
var mysqlValuesExp *regexp.Regexp
var pgsqlValuesExp *regexp.Regexp

var configFilename string

func init() {
	useExp = regexp.MustCompile("^USE `(.+)`;")
	connectExp = regexp.MustCompile("^\\\\connect (.+)")
	mysqlValuesExp = regexp.MustCompile("^INSERT INTO `(.+?)` VALUES \\((.+)\\);")
	pgsqlValuesExp = regexp.MustCompile("^INSERT INTO (.+?) VALUES \\((.+)\\);")
}

type options struct {
	Host string
	Port int
	Ssl  bool
}

type SqlConfig struct {
	Tables map[string]TableInfo
}

type TableInfo struct {
	TableName   string
	Measurement string
	SqlFields   [][]interface{}
}

func GetCommand() *cobra.Command {
	c := &cobra.Command{
		Use:     "sql2cnosdb [path] [Use]",
		Short:   "Import a dump-sql file to cnosdb",
		Long:    "Import a dump-sql file to cnosdb",
		Example: "refer import command",
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

			tables := &SqlConfig{}
			if _, err := toml.DecodeFile(configFilename, tables); err != nil {
				fmt.Printf("[ERR] %s\n", err)
				return
			}

			for k, v := range tables.Tables {
				if len(v.SqlFields) != 3 {
					fmt.Printf("[ERR] %s config field array not right\n", k)
					return
				}

				for i := 0; i < len(v.SqlFields); i++ {
					if len(v.SqlFields[i]) <= 0 {
						fmt.Printf("[ERR] %s config fields name not right\n", k)
						return
					}

					if len(v.SqlFields[i]) != len(v.SqlFields[0]) {
						fmt.Printf("[ERR] %s config fields not right\n", k)
						return
					}
				}
			}

			reader, writer := io.Pipe()
			go func() {
				defer writer.Close()
				if err := parseSqlDumpFile(config.Path, tables.Tables, writer); err != nil {
					fmt.Printf("[ERR] %s\n", err)
				}
			}()

			defer reader.Close()
			i := importer.NewImporter(*config)
			if err := i.Import(reader); err != nil {
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

	flags.StringVar(&config.Precision, "precision", "ns", "Precision specifies the format of the timestamp:  rfc3339,h,m,s,ms,u or ns.")
	flags.StringVar(&config.WriteConsistency, "consistency", "all", "Set write consistency level: any, one, quorum, or all.")
	flags.StringVar(&config.Path, "path", "", "Path to the file to import.")
	flags.IntVar(&config.PPS, "pps", 0, "How many points per second the import will allow.  By default it is zero and will not throttle importing.")
	flags.BoolVar(&config.Compressed, "compressed", false, "set to true if the import file is compressed")

	flags.StringVar(&configFilename, "config", "", "import sql file to cnosdb config file.")

	return c
}

func parseSqlDumpFile(filename string, tables map[string]TableInfo, writer io.Writer) error {
	file, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	dbName := ""
	tableName := ""
	scanner := bufio.NewReader(file)

	io.WriteString(writer, "# DML\n")
	for {
		line, err := scanner.ReadString('\n')
		if err == io.EOF {
			return nil
		} else if err != nil {
			return err
		}
		line = strings.Trim(line, "\r\n")

		if m := useExp.FindAllStringSubmatch(line, -1); len(m) == 1 {
			dbName = m[0][1]
			io.WriteString(writer, "# CONTEXT-DATABASE: "+dbName+"\n")
		}
		if m := connectExp.FindAllStringSubmatch(line, -1); len(m) == 1 {
			dbName = m[0][1]
			io.WriteString(writer, "# CONTEXT-DATABASE: "+dbName+"\n")
		}

		match := mysqlValuesExp.FindAllStringSubmatch(line, -1)
		if len(match) != 1 {
			match = pgsqlValuesExp.FindAllStringSubmatch(line, -1)
			if len(match) != 1 {
				continue
			}
		}

		tableName = match[0][1]

		fmt.Printf("\nSQL [%s.%s] %s\n", dbName, tableName, line)

		dbName, measName, fields := getDbnameMeasurement(dbName, tableName, tables)
		if dbName == "" || measName == "" || fields == nil {
			continue
		}

		values, err := parseValues(match[0][2])
		if err != nil {
			return fmt.Errorf("parse line failed: %v", line)
		}

		if len(values)%len(fields[0]) != 0 {
			return fmt.Errorf("line fields not right: %v", line)
		}

		for i := 0; i < len(values); i = i + len(fields[0]) {
			str, err := convertSql2LineProtocol(values[i:i+len(fields[0])], fields)
			if err != nil {
				return fmt.Errorf("can't convert to line protocal %v | %s", err, line)
			}

			lineProto := "insert " + measName + "," + str
			io.WriteString(writer, measName+","+str+"\n")
			fmt.Printf("Line [%s], %s\n", dbName, lineProto)
		}
	}
}

func convertSql2LineProtocol(strs []string, ftype [][]interface{}) (string, error) {
	tags := ""
	fields := ""
	timestamp := ""

	for i := 0; i < len(strs); i++ {
		t := ftype[2][i].(int64)
		if t == 1 {
			timestamp = strs[i]
		} else if t == 2 {
			tags += ftype[1][i].(string) + "=" + strs[i] + ","
		} else if t == 3 {
			fields += ftype[1][i].(string) + "=" + replacePrefixOrSuffix(strs[i], "'", "\"") + ","
		}
	}

	if len(tags) == 0 || len(fields) == 0 {
		return "", fmt.Errorf("tags or fields empty")
	}
	tags = tags[0 : len(tags)-1]
	fields = fields[0 : len(fields)-1]

	timestamp = replacePrefixOrSuffix(timestamp, "'", "")
	theTime, err := time.Parse("2006-01-02 15:04:05", timestamp)
	if err != nil {
		return "", err
	}

	return tags + " " + fields + " " + strconv.FormatInt(theTime.UnixNano(), 10), nil
}

func getDbnameMeasurement(dbName, tabName string, tables map[string]TableInfo) (string, string, [][]interface{}) {
	for _, v := range tables {
		strs := strings.Split(v.TableName, "/")
		if len(strs) != 2 {
			continue
		}
		sqlDbName := strs[0]
		sqlTabName := strs[1]

		strs = strings.Split(v.Measurement, "/")
		if len(strs) != 2 {
			continue
		}
		cnosdbDbName := strs[0]
		cnosdbMeas := strs[1]

		realDbName := matchValue(dbName, sqlDbName, cnosdbDbName)
		realMeasName := matchValue(tabName, sqlTabName, cnosdbMeas)
		if realDbName != "" && realMeasName != "" {
			return realDbName, realMeasName, v.SqlFields
		}

	}
	return "", "", nil
}

func matchValue(value, pattern, dest string) string {
	if value == pattern ||
		(strings.HasPrefix(pattern, "*") && strings.HasSuffix(value, pattern[1:])) ||
		(strings.HasSuffix(pattern, "*") && strings.HasPrefix(value, pattern[0:len(pattern)-1])) ||
		(strings.HasPrefix(pattern, "*") && strings.HasSuffix(pattern, "*") && strings.Index(value, pattern[1:len(pattern)-1]) >= 0) {
		if strHasPrefixOrSuffix(dest, "*") {
			return strings.ReplaceAll(value, ".", "_")
		} else {
			return dest
		}
	}

	return ""
}

func replacePrefixOrSuffix(s, old, new string) string {
	if strings.HasPrefix(s, old) {
		s = new + s[len(old):]
	}

	if strings.HasSuffix(s, old) {
		s = s[0:len(s)-len(old)] + new
	}

	return s
}
func strHasPrefixOrSuffix(s, sub string) bool {
	if strings.HasPrefix(s, sub) || strings.HasSuffix(s, sub) {
		return true
	} else {
		return false
	}
}

func parseValues(str string) ([]string, error) {
	values := make([]string, 0, 8)

	i := 0
	for i < len(str) {
		if str[i] == '\'' {
			// read string until another single quote
			j := i + 1

			escaped := false
			for j < len(str) {
				if str[j] == '\\' {
					// skip escaped character
					j += 2
					escaped = true
					continue
				} else if str[j] == '\'' {
					break
				} else {
					j++
				}
			}

			if j >= len(str) {
				return nil, fmt.Errorf("parse quote values error")
			}

			value := str[i : j+1]
			if escaped {
				value = unescapeString(value)
			}
			values = append(values, value)
			// skip ' and (, or )  )
			i = j + 2
		} else if str[i] == '(' || str[i] == ',' || str[i] == ')' || str[i] == ' ' {
			i = i + 1
		} else {
			// no string, read until comma
			j := i + 1
			for ; j < len(str) && str[j] != ',' && str[j] != ')'; j++ {
			}
			values = append(values, str[i:j])
			// skip , )
			i = j + 1
		}

		// need skip blank???
	}

	return values, nil
}

func unescapeString(s string) string {
	i := 0

	value := make([]byte, 0, len(s))
	for i < len(s) {
		if s[i] == '\\' {
			j := i + 1
			if j == len(s) {
				// The last char is \, remove
				break
			}

			value = append(value, unescapeChar(s[j]))
			i += 2
		} else {
			value = append(value, s[i])
			i++
		}
	}

	return string(value)
}

func unescapeChar(ch byte) byte {
	// \" \' \\ \n \0 \b \Z \r \t ==> escape to one char
	switch ch {
	case 'n':
		ch = '\n'
	case '0':
		ch = 0
	case 'b':
		ch = 8
	case 'Z':
		ch = 26
	case 'r':
		ch = '\r'
	case 't':
		ch = '\t'
	}
	return ch
}

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
			return url.URL{}, fmt.Errorf("invalid port number %s: %s\n", path, err)
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
