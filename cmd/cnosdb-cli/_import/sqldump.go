package _import

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/cnosdb/cnosdb/pkg/importer"
)

var useExp *regexp.Regexp
var connectExp *regexp.Regexp
var mysqlValuesExp *regexp.Regexp
var pgsqlValuesExp *regexp.Regexp

func init() {
	useExp = regexp.MustCompile("^USE `(.+)`;")
	connectExp = regexp.MustCompile("^\\\\connect (.+)")
	mysqlValuesExp = regexp.MustCompile("^INSERT INTO `(.+?)` VALUES \\((.+)\\);")
	pgsqlValuesExp = regexp.MustCompile("^INSERT INTO (.+?) VALUES \\((.+)\\);")
}

type SqlConfig struct {
	Tables map[string]TableInfo
}

type TableInfo struct {
	TableName   string
	Measurement string
	SqlFields   [][]interface{}
}

func importSqlDumpData(config *importer.Config) error {
	tables, err := parseConfigFile(config.ConfigFile)
	if err != nil {
		return err
	}

	reader, writer := io.Pipe()
	defer reader.Close()
	go func() {
		if err := parseSqlDumpFile(config.Path, tables, writer); err != nil {
			writer.CloseWithError(err)
		} else {
			writer.Close()
		}
	}()

	i := importer.NewImporter(*config)
	return i.Import(reader)
}

func parseConfigFile(name string) (map[string]TableInfo, error) {
	config := &SqlConfig{}
	if _, err := toml.DecodeFile(name, config); err != nil {
		return nil, err
	}

	for k, v := range config.Tables {
		if len(v.SqlFields) != 3 {
			return nil, fmt.Errorf("%s config field array not right", k)

		}

		if len(v.SqlFields[0]) != len(v.SqlFields[1]) || len(v.SqlFields[0]) != len(v.SqlFields[2]) {
			return nil, fmt.Errorf("%s config fields not right", k)
		}

		for i := 0; i < len(v.SqlFields[2]); i++ {
			val := v.SqlFields[2][i].(int64)
			if val <= 0 || val > 3 {
				continue
			}

			if v.SqlFields[1][i] == "" {
				return nil, fmt.Errorf("%s cnosdb field name can't be empty", k)
			}
		}
	}

	return config.Tables, nil
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
		//fmt.Printf("\nSQL [%s.%s] %s\n", dbName, tableName, line)
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

			io.WriteString(writer, measName+","+str+"\n")
			//fmt.Printf("Line [%s], %s\n", dbName, "insert " + measName + "," + str)
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
