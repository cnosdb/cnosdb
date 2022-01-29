package cli

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/cnosdatabase/cnosdb/client"
	"github.com/cnosdatabase/cnosdb/pkg/utils"
	"github.com/cnosdatabase/cnosql"
	"github.com/cnosdatabase/db/models"

	"github.com/peterh/liner"
	"github.com/pkg/errors"
)

var ErrBlankCommand = errors.New("empty input")

type CommandLine struct {
	Host      string
	Port      int
	addr      string
	Ssl       bool
	NodeID    int
	Format    string
	Chunked   bool
	ChunkSize int
	Pretty    bool

	client        client.Client
	clientConfig  *client.HTTPConfig
	pointConfig   *client.BatchPointsConfig
	clientVersion string
	serverVersion string

	line            *liner.State
	historyFilePath string

	osSignals chan os.Signal
	quit      chan struct{}
}

func newCommandLine(version string) *CommandLine {
	return &CommandLine{
		clientConfig:  &client.HTTPConfig{},
		pointConfig:   &client.BatchPointsConfig{},
		clientVersion: version,
		quit:          make(chan struct{}, 1),
		osSignals:     make(chan os.Signal, 1),
		Chunked:       true,
	}
}

func (c *CommandLine) Run() error {
	if err := c.setEnv(); err != nil {
		return err
	}

	c.line = liner.NewLiner()
	defer func(line *liner.State) {
		if err := line.Close(); err != nil {
			fmt.Printf("ERR: cannot close the Liner: %s\n", err)
		}
	}(c.line)

	c.setHistoryPath()
	c.loadHistory()

	if err := c.connect(""); err != nil {
		return err
	}
	return c.mainLoop()
}

func (c *CommandLine) setEnv() error {
	if c.clientConfig.Username == "" {
		c.clientConfig.Username = os.Getenv("CNOSDB_USERNAME")
	}
	if promptForPassword {
		p, e := func() (string, error) {
			l := liner.NewLiner()
			defer func(line *liner.State) {
				if err := line.Close(); err != nil {
					fmt.Printf("ERR: cannot close the Liner: %s\n", err)
				}
			}(l)
			return l.PasswordPrompt("password: ")
		}()
		if e != nil {
			return errors.New("Unable to parse password")
		}
		c.clientConfig.Password = p
	} else if c.clientConfig.Password == "" {
		c.clientConfig.Password = os.Getenv("CNOSDB_PASSWORD")
	}
	return nil
}

func (c *CommandLine) mainLoop() error {
	for {
		select {
		case <-c.osSignals:
			c.exit()
			return nil
		case <-c.quit:
			c.exit()
			return nil
		default:
			l, e := c.line.Prompt("> ")
			if e == io.EOF {
				// 转换为一条 exit 命令
				l = "exit"
			} else if e != nil {
				c.exit()
				return e
			}
			if err := c.parseCommand(l); err != ErrBlankCommand && !strings.HasPrefix(strings.TrimSpace(l), "auth") {
				l = utils.Sanitize(l)
				c.line.AppendHistory(l)
				c.saveHistory()
			}
		}
	}
}

func (c *CommandLine) parseCommand(cmd string) error {
	tokens := strings.Fields(strings.TrimSpace(strings.ToLower(cmd)))
	if len(tokens) > 0 {
		switch tokens[0] {
		case "exit", "quit":
			close(c.quit)
		case "connect":
			return c.connect(cmd)
		case "logo":
			c.logo()
		case "auth":
			c.setAuth(cmd)
		case "chunked":
			c.Chunked = !c.Chunked
			if c.Chunked {
				fmt.Println("chunked responses enabled")
			} else {
				fmt.Println("chunked reponses disabled")
			}
		case "chunk":
			c.setChunkSize(cmd)
		case "help":
			c.printHelp()
		case "history":
			c.printHistory()
		case "format":
			c.setFormat(cmd)
		case "precision":
			c.setPrecision(cmd)
		case "pretty":
			c.Pretty = !c.Pretty
			if c.Pretty {
				fmt.Println("Pretty print enabled")
			} else {
				fmt.Println("Pretty print disabled")
			}
		case "settings":
			c.printSettings()
		case "use":
			c.setDatabaseAndRP(cmd)
		case "node":
			c.setNode(cmd)
		case "insert":
			return c.requestInsert(cmd)
		case "clear":
			c.clear(cmd)
		default:
			return c.requestQuery(cmd)
		}

		return ErrBlankCommand
	}
	return nil
}

// connect 处理连接命令： connect HOST:PORT
func (c *CommandLine) connect(cmd string) error {
	cmd = strings.ToLower(cmd)

	var addr string
	if cmd == "" {
		// connect 命令为空时，使用当前配置
		addr = net.JoinHostPort(c.Host, strconv.Itoa(c.Port))
	} else {
		addr = strings.TrimSpace(strings.Replace(cmd, "connect", "", 1))
	}

	URL, err := parseConnectionString(addr, c.Ssl)
	if err != nil {
		return err
	}

	cfg := c.clientConfig
	cfg.UserAgent = "CnosDBShell/" + c.clientVersion
	cfg.Addr = URL.String()
	cfg.Proxy = http.ProxyFromEnvironment

	if c.client != nil {
		_ = c.client.Close()
	}

	cli1, err := client.NewHTTPClient(*cfg)
	if err != nil {
		return fmt.Errorf("could not create client: %s", err)
	}
	c.client = cli1

	_, v, err := c.client.Ping(10 * time.Second)
	if err != nil {
		return err
	}
	c.serverVersion = v

	// 更新配置信息
	if host, port, err := net.SplitHostPort(cfg.Addr); err == nil {
		c.Host = host
		if i, err := strconv.Atoi(port); err == nil {
			c.Port = i
		}
	}

	return nil
}

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

// 处理设置鉴权信息命令： auth <username> <password>
func (c *CommandLine) setAuth(cmd string) {
	args := strings.Fields(cmd)
	if len(args) == 3 {
		args = args[1:]
	} else {
		args = []string{}
	}

	if len(args) == 2 {
		c.clientConfig.Username = args[0]
		c.clientConfig.Password = args[1]
	} else {
		u, e := c.line.Prompt("username: ")
		if e != nil {
			fmt.Printf("ERR: unable to process input: %s\n", e)
			return
		}
		c.clientConfig.Username = strings.TrimSpace(u)
		p, e := c.line.PasswordPrompt("password: ")
		if e != nil {
			fmt.Printf("ERR: unable to process input: %s\n", e)
			return
		}
		c.clientConfig.Password = p
	}

	// 更新配置
	cli, err := client.NewHTTPClient(*c.clientConfig)
	if err != nil {
		fmt.Printf("ERR: could not create client: %s\n", err)
	} else {
		if c.client != nil {
			_ = c.client.Close()
		}
		c.client = cli
	}

}

// 处理设置输出格式命令： format <json | csv | column>
func (c *CommandLine) setFormat(cmd string) {
	cmd = strings.ToLower(cmd)
	// Remove the "format" keyword if it exists
	cmd = strings.TrimSpace(strings.Replace(cmd, "format", "", -1))

	switch cmd {
	case "json", "csv", "column":
		c.Format = cmd
	default:
		fmt.Printf("ERR: unknown format %q. Please use json, csv, or column.\n", cmd)
	}
}

// setChunkSize 设置缓冲块大小，设置为 0 表示使用默认值
func (c *CommandLine) setChunkSize(cmd string) {
	// normalize cmd
	cmd = strings.ToLower(cmd)
	cmd = strings.Join(strings.Fields(cmd), " ")

	// Remove the "chunk size" keyword if it exists
	cmd = strings.TrimPrefix(cmd, "chunk size ")

	// Remove the "chunk" keyword if it exists
	// allows them to use `chunk 50` as a shortcut
	cmd = strings.TrimPrefix(cmd, "chunk ")

	if n, err := strconv.ParseInt(cmd, 10, 64); err == nil {
		c.ChunkSize = int(n)
		if c.ChunkSize <= 0 {
			c.ChunkSize = 0
		}
		fmt.Printf("chunk size set to %d\n", c.ChunkSize)
	} else {
		fmt.Printf("unable to parse chunk size from %q\n", cmd)
	}
}

// setPrecision 设置时间精度命令： precision <Precision>
func (c *CommandLine) setPrecision(cmd string) {
	// normalize cmd
	cmd = strings.ToLower(cmd)

	// Remove the "precision" keyword if it exists
	cmd = strings.TrimSpace(strings.Replace(cmd, "precision", "", -1))

	switch cmd {
	case "h", "m", "s", "ms", "u", "ns":
		c.pointConfig.Precision = cmd
	case "rfc3339":
		c.pointConfig.Precision = ""
	default:
		fmt.Printf("ERR: unknown precision %q. Please use rfc3339, h, m, s, ms, u or ns.\n", cmd)
	}
}

// setDatabaseAndRP 设置默认数据库命令： use <Database>
func (c *CommandLine) setDatabaseAndRP(cmd string) {
	args := strings.SplitAfterN(strings.TrimSuffix(strings.TrimSpace(cmd), ";"), " ", 2)
	if len(args) != 2 {
		fmt.Printf("ERR: could not parse database name from %q.\n", cmd)
		return
	}

	stmt := args[1]
	db, rp, err := parseDatabaseAndRetentionPolicy([]byte(stmt))
	if err != nil {
		fmt.Printf("ERR: unable to parse database or rp from %s", stmt)
		return
	}

	if !c.requestDbExists(db) {
		fmt.Printf("ERR: database %s does not exist!\n", db)
		return
	}

	c.pointConfig.Database = db
	fmt.Printf("Using database %s\n", db)

	if rp != "" {
		if !c.requestRpExists(db, rp) {
			return
		}
		c.pointConfig.RetentionPolicy = rp
		fmt.Printf("Using rp %s\n", rp)
	}
}

func parseDatabaseAndRetentionPolicy(stmt []byte) (string, string, error) {
	var db, rp []byte
	var quoted bool
	var separatorCount int

	stmt = bytes.TrimSpace(stmt)

	for _, b := range stmt {
		if b == '"' {
			quoted = !quoted
			continue
		}
		if b == '.' && !quoted {
			separatorCount++
			if separatorCount > 1 {
				return "", "", errors.Errorf("unable to parse database and rp from %s", string(stmt))
			}
			continue
		}
		if separatorCount == 1 {
			rp = append(rp, b)
			continue
		}
		db = append(db, b)
	}
	return string(db), string(rp), nil
}

// requestDbExists Server交互：检查数据库是否存在
func (c *CommandLine) requestDbExists(db string) bool {
	response, err := c.client.Query(client.Query{Command: "SHOW DATABASES"})
	if err != nil {
		fmt.Printf("ERR: %s\n", err)
		return false
	} else if err := response.Error(); err != nil {
		if c.clientConfig.Username == "" {
			fmt.Printf("ERR: %s\n", err)
			return false
		}
		fmt.Printf("WARN: %s\n", err)
	} else {
		if databaseExists := func() bool {
			for _, result := range response.Results {
				for _, row := range result.Series {
					if row.Name == "databases" {
						for _, values := range row.Values {
							for _, database := range values {
								if database == db {
									return true
								}
							}
						}
					}
				}
			}
			return false
		}(); !databaseExists {
			fmt.Printf("ERR: database %s doesn't exist. Run SHOW DATABASES for a list of existing databases.\n", db)
			return false
		}
	}
	return true
}

// requestRpExists Server 交互：检查保留策略是否存在
func (c *CommandLine) requestRpExists(db, rp string) bool {
	response, err := c.client.Query(client.Query{Command: fmt.Sprintf("SHOW RETENTION POLICIES ON %q", db)})
	if err != nil {
		fmt.Printf("ERR: %s\n", err)
		return false
	} else if err := response.Error(); err != nil {
		if c.clientConfig.Username == "" {
			fmt.Printf("ERR: %s\n", err)
			return false
		}
		fmt.Printf("WARN: %s\n", err)
	} else {
		if retentionPolicyExists := func() bool {
			for _, result := range response.Results {
				for _, row := range result.Series {
					for _, values := range row.Values {
						for i, v := range values {
							if i != 0 {
								continue
							}
							if v == rp {
								return true
							}
						}
					}
				}
			}
			return false
		}(); !retentionPolicyExists {
			fmt.Printf("ERR: RP %s doesn't exist. Run SHOW RPS ON %q for a list of existing rps.\n", rp, db)
			return false
		}
	}
	return true
}

// setNode 设置默认数据服务器命令： node <Node>
func (c *CommandLine) setNode(cmd string) {
	args := strings.Split(strings.TrimSuffix(strings.TrimSpace(cmd), ";"), " ")
	if len(args) != 2 {
		fmt.Println("ERR: improper number of arguments for 'node' command, requires exactly one.")
		return
	}

	if args[1] == "clear" {
		c.NodeID = 0
		return
	}

	id, err := strconv.Atoi(args[1])
	if err != nil {
		fmt.Printf("ERR: unable to parse node id from %s. Must be an integer or 'clear'.\n", args[1])
		return
	}
	c.NodeID = id
}

// requestInsert Server 交互：执行写入请求
func (c *CommandLine) requestInsert(cmd string) error {
	bp, err := c.parseInsert(cmd)
	if err != nil {
		fmt.Printf("ERR: %s\n", err)
		return nil
	}
	if err := c.client.Write(bp); err != nil {
		fmt.Printf("ERR: %s\n", err)
		if c.pointConfig.Database == "" {
			fmt.Println("Note: error may be due to not setting a database or rp.")
			fmt.Println(`Please set a database with the command "use <database>" or`)
			fmt.Println("INSERT INTO <database>.<retention policy> <point>")
		}
	}
	return nil
}

// query 创建 client.Query 实例
func (c *CommandLine) query(query string) client.Query {
	return client.Query{
		Command:         query,
		Database:        c.pointConfig.Database,
		RetentionPolicy: c.pointConfig.RetentionPolicy,
		Chunked:         c.Chunked,
		ChunkSize:       c.ChunkSize,
	}
}

// requestQuery Server 交互：执行查询请求
func (c *CommandLine) requestQuery(query string) error {
	// 以默认的 rp 覆盖输入语句中的 rp （如果设置了）
	if c.pointConfig.RetentionPolicy != "" {
		pq, err := cnosql.NewParser(strings.NewReader(query)).ParseQuery()
		if err != nil {
			fmt.Printf("ERR: %s\n", err)
			return err
		}
		for _, stmt := range pq.Statements {
			if selectStatement, ok := stmt.(*cnosql.SelectStatement); ok {
				cnosql.WalkFunc(selectStatement.Sources, func(n cnosql.Node) {
					if t, ok := n.(*cnosql.Measurement); ok {
						if t.Database == "" && c.pointConfig.Database != "" {
							t.Database = c.pointConfig.Database
						}
						if t.RetentionPolicy == "" && c.pointConfig.RetentionPolicy != "" {
							t.RetentionPolicy = c.pointConfig.RetentionPolicy
						}
					}
				})
			}
		}
		query = pq.String()
	}

	response, err := c.client.Query(c.query(query))
	if err != nil {
		fmt.Printf("ERR: %s\n", err)
		return err
	}
	c.writeResponse(response, os.Stdout)
	if err := response.Error(); err != nil {
		fmt.Printf("ERR: %s\n", response.Error())
		if c.pointConfig.Database == "" {
			fmt.Println("Warning: It is possible this error is due to not setting a database.")
			fmt.Println(`Please set a database with the command "use <database>".`)
		}
		return err
	}
	return nil
}

func parseNextIdentifier(stmt string) (ident, remainder string) {
	if len(stmt) > 0 {
		switch {
		case isWhitespace(rune(stmt[0])):
			return parseNextIdentifier(stmt[1:])
		case isIdentFirstChar(rune(stmt[0])):
			return parseUnquotedIdentifier(stmt)
		case stmt[0] == '"':
			return parseDoubleQuotedIdentifier(stmt)
		}
	}
	return "", stmt
}

func parseUnquotedIdentifier(stmt string) (string, string) {
	if fields := strings.FieldsFunc(stmt, isNotIdentChar); len(fields) > 0 {
		return fields[0], strings.TrimPrefix(stmt, fields[0])
	}
	return "", stmt
}

func parseDoubleQuotedIdentifier(stmt string) (string, string) {
	escapeNext := false
	fields := strings.FieldsFunc(stmt, func(ch rune) bool {
		if ch == '\\' {
			escapeNext = true
		} else if ch == '"' {
			if !escapeNext {
				return true
			}
			escapeNext = false
		}
		return false
	})
	if len(fields) > 0 {
		return fields[0], strings.TrimPrefix(stmt, "\""+fields[0]+"\"")
	}
	return "", stmt
}

func (c *CommandLine) parseInsert(stmt string) (client.BatchPoints, error) {
	ident, point := parseNextIdentifier(stmt)
	if !strings.EqualFold(ident, "insert") {
		return nil, fmt.Errorf("found %s, expected INSERT", ident)
	}

	var bpCfg *client.BatchPointsConfig
	if ident, r := parseNextIdentifier(point); strings.EqualFold(ident, "into") {
		bpCfg = c.parseInto(r)
	} else {
		bpCfg = c.pointConfig
	}

	ps, err := c.parsePoints(point)
	if err != nil {
		return nil, errors.Unwrap(err)
	}

	bp, err := client.NewBatchPoints(*bpCfg)
	if err != nil {
		return nil, errors.Unwrap(err)
	}
	bp.AddPoints(ps)

	return bp, nil
}

func (c *CommandLine) parseInto(stmt string) *client.BatchPointsConfig {
	ident, stmt := parseNextIdentifier(stmt)
	db, rp := c.pointConfig.Database, c.pointConfig.RetentionPolicy
	if strings.HasPrefix(stmt, ".") {
		db = ident
		ident, stmt = parseNextIdentifier(stmt[1:])
	}
	if strings.HasPrefix(stmt, " ") {
		rp = ident
		stmt = stmt[1:]
	}

	return &client.BatchPointsConfig{
		Database:         db,
		RetentionPolicy:  rp,
		Precision:        c.pointConfig.Precision,
		WriteConsistency: c.pointConfig.WriteConsistency,
	}
}

func (c *CommandLine) parsePoints(point string) ([]*client.Point, error) {
	mps, err := models.ParsePoints([]byte(point))
	if err != nil || len(mps) == 0 {
		return nil, errors.Unwrap(err)
	}

	var points = make([]*client.Point, len(mps))
	for i, mp := range mps {
		points[i] = client.NewPointFrom(mp)
	}

	return points, nil
}

// writeResponse 输出结果集
func (c *CommandLine) writeResponse(response *client.Response, w io.Writer) {
	switch c.Format {
	case "json":
		c.writeJSON(response, w)
	case "csv":
		c.writeCSV(response, w)
	case "column":
		c.writeColumns(response, w)
	default:
		_, _ = fmt.Fprintf(w, "Unknown output format %q.\n", c.Format)
	}
}

func tagsEqual(prev, current map[string]string) bool {
	return reflect.DeepEqual(prev, current)
}

func columnsEqual(prev, current []string) bool {
	return reflect.DeepEqual(prev, current)
}

func headersEqual(prev, current models.Row) bool {
	if prev.Name != current.Name {
		return false
	}
	return tagsEqual(prev.Tags, current.Tags) && columnsEqual(prev.Columns, current.Columns)
}

func interfaceToString(v interface{}) string {
	switch t := v.(type) {
	case nil:
		return ""
	case bool:
		return fmt.Sprintf("%v", v)
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, uintptr:
		return fmt.Sprintf("%d", t)
	case float32, float64:
		return fmt.Sprintf("%v", t)
	default:
		return fmt.Sprintf("%v", t)
	}
}

// formatResultSeries 格式化结果集，对于 csv 和 column 格式有不同的输出结果
func (c *CommandLine) formatResultSeries(result client.Result, separator string, suppressHeaders bool) []string {
	var rows []string
	for i, row := range result.Series {
		var tags []string
		for k, v := range row.Tags {
			tags = append(tags, fmt.Sprintf("%s=%s", k, v))
		}
		sort.Strings(tags)

		var columnNames []string

		// 对于 csv 格式，将 name 和 tag 作为列输出
		if c.Format == "csv" {
			if len(tags) > 0 {
				columnNames = append([]string{"tags"}, columnNames...)
			}

			if row.Name != "" {
				columnNames = append([]string{"name"}, columnNames...)
			}
		}

		columnNames = append(columnNames, row.Columns...)

		// 对于 column 格式，若结果集中存在多个序列条目，不同的序列条目间输出空行
		if i > 0 && c.Format == "column" && !suppressHeaders {
			rows = append(rows, "")
		}

		// 对于 column 格式，将 name 和 tag 各自输出到单独的行
		if c.Format == "column" && !suppressHeaders {
			if row.Name != "" {
				n := fmt.Sprintf("name: %s", row.Name)
				rows = append(rows, n)
			}
			if len(tags) > 0 {
				t := fmt.Sprintf("tags: %s", (strings.Join(tags, ", ")))
				rows = append(rows, t)
			}
		}

		if !suppressHeaders {
			rows = append(rows, strings.Join(columnNames, separator))
		}

		// 对于 column 模式，在列名下输出 "-"
		if c.Format == "column" && !suppressHeaders {
			lines := make([]string, len(columnNames))
			for colIdx, colName := range columnNames {
				lines[colIdx] = strings.Repeat("-", len(colName))
			}
			rows = append(rows, strings.Join(lines, separator))
		}

		for _, v := range row.Values {
			var values []string
			if c.Format == "csv" {
				if row.Name != "" {
					values = append(values, row.Name)
				}
				if len(tags) > 0 {
					values = append(values, strings.Join(tags, ","))
				}
			}

			for _, vv := range v {
				values = append(values, interfaceToString(vv))
			}
			rows = append(rows, strings.Join(values, separator))
		}
	}
	return rows
}

func (c *CommandLine) writeCSV(response *client.Response, w io.Writer) {
	cw := csv.NewWriter(w)
	var previousHeaders models.Row
	for _, result := range response.Results {
		suppressHeaders := len(result.Series) > 0 && headersEqual(previousHeaders, result.Series[0])
		if !suppressHeaders && len(result.Series) > 0 {
			previousHeaders = models.Row{
				Name:    result.Series[0].Name,
				Tags:    result.Series[0].Tags,
				Columns: result.Series[0].Columns,
			}
		}

		rows := c.formatResultSeries(result, "\t", suppressHeaders)
		for _, r := range rows {
			_ = cw.Write(strings.Split(r, "\t"))
		}
	}
	cw.Flush()
}

func (c *CommandLine) writeColumns(response *client.Response, w io.Writer) {
	writer := new(tabwriter.Writer)
	writer.Init(w, 0, 8, 1, ' ', 0)

	var previousHeaders models.Row
	for i, result := range response.Results {
		// 1. 输出 Messages
		for _, m := range result.Messages {
			_, _ = fmt.Fprintf(w, "%s: %s.\n", m.Level, m.Text)
		}
		// Check to see if the headers are the same as the previous row.  If so, suppress them in the output
		suppressHeaders := len(result.Series) > 0 && headersEqual(previousHeaders, result.Series[0])
		if !suppressHeaders && len(result.Series) > 0 {
			previousHeaders = models.Row{
				Name:    result.Series[0].Name,
				Tags:    result.Series[0].Tags,
				Columns: result.Series[0].Columns,
			}
		}

		// If we are suppressing headers, don't output the extra line return. If we
		// aren't suppressing headers, then we put out line returns between results
		// (not before the first result, and not after the last result).
		if !suppressHeaders && i > 0 {
			_, _ = fmt.Fprintln(writer, "")
		}

		rows := c.formatResultSeries(result, "\t", suppressHeaders)
		for _, r := range rows {
			_, _ = fmt.Fprintln(writer, r)
		}

	}
	_ = writer.Flush()
}

func (c *CommandLine) writeJSON(response *client.Response, w io.Writer) {
	var d []byte
	var err error
	if c.Pretty {
		d, err = json.MarshalIndent(response, "", "    ")
	} else {
		d, err = json.Marshal(response)
	}
	if err != nil {
		_, _ = fmt.Fprintf(w, "ERR: unable to parse json: %s\n", err)
		return
	}
	_, _ = fmt.Fprintln(w, string(d))
}

func (c *CommandLine) clear(cmd string) {
	args := strings.Split(strings.TrimSuffix(strings.TrimSpace(cmd), ";"), " ")
	v := strings.ToLower(strings.Join(args[1:], " "))
	switch v {
	case "database", "db":
		c.pointConfig.Database = ""
		fmt.Println("Database context cleared")
		return
	case "retention policy", "rp":
		c.pointConfig.RetentionPolicy = ""
		fmt.Println("Retention Policy context cleared")
		return
	default:
		if len(args) > 1 {
			fmt.Printf("ERR: invalid command %q.\n", v)
		}
		fmt.Print(`Note: Possible commands for 'clear' are:
    # Clear the database context
    clear database
    clear db

    # Clear the retention policy context
    clear retention policy
    clear rp
`)
	}
}

func (c *CommandLine) printHelp() {
	fmt.Println(`Usage:
        connect <host:port>   connects to another node specified by host:port
        auth                  prompts for username and password
        pretty                toggles pretty print for the json format
        chunked               turns on chunked responses from server
        chunk size <size>     sets the size of the chunked responses.  Set to 0 to reset to the default chunked size
        use <db_name>         sets current database
        format <format>       specifies the format of the server responses: json, csv, or column
        precision <format>    specifies the format of the timestamp: rfc3339, h, m, s, ms, u or ns
        consistency <level>   sets write consistency level: any, one, quorum, or all
        history               displays command history
        settings              outputs the current settings for the shell
        clear                 clears settings such as database or retention policy.  run 'clear' for help
        exit/quit/ctrl+d      quits the cnosdb-cli shell

        show databases        show database names
        show series           show series information
        show measurements     show measurement information
        show tag keys         show tag key information
        show field keys       show field key information
	logo                  display producer logo`)
}

func (c *CommandLine) printSettings() {
	w := new(tabwriter.Writer)
	w.Init(os.Stdout, 0, 1, 1, ' ', 0)
	_, _ = fmt.Fprintln(w, "Setting\tValue")
	_, _ = fmt.Fprintln(w, "--------\t--------")
	_, _ = fmt.Fprintf(w, "Addr\t%s\n", c.clientConfig.Addr)
	_, _ = fmt.Fprintf(w, "Username\t%s\n", c.clientConfig.Username)
	_, _ = fmt.Fprintf(w, "Database\t%s\n", c.pointConfig.Database)
	_, _ = fmt.Fprintf(w, "Retention Policy\t%s\n", c.pointConfig.RetentionPolicy)
	_, _ = fmt.Fprintf(w, "Format\t%s\n", c.Format)
	_, _ = fmt.Fprintf(w, "Precision\t%s\n", c.pointConfig.Precision)
	_, _ = fmt.Fprintf(w, "Chunked\t%v\n", c.Chunked)
	_, _ = fmt.Fprintf(w, "Chunk Size\t%d\n", c.ChunkSize)
	_ = w.Flush()
}

func (c *CommandLine) logo() {
	logo, err := ioutil.ReadFile("cmd/cnosdb-cli/cli/logo")
	if err != nil {
		panic(err)
	}
	fmt.Println(string(logo))
}

func (c *CommandLine) setHistoryPath() {
	var historyDir string
	if runtime.GOOS == "windows" {
		if userDir := os.Getenv("USERPROFILE"); userDir != "" {
			historyDir = userDir
		}
	}

	if homeDir := os.Getenv("HOME"); homeDir != "" {
		historyDir = homeDir
	}

	if historyDir != "" {
		c.historyFilePath = filepath.Join(historyDir, ".cnosdb_history")
	}
}

func (c *CommandLine) loadHistory() {
	if c.historyFilePath == "" {
		return
	}
	if historyFile, err := os.Open(c.historyFilePath); err == nil {
		_, _ = c.line.ReadHistory(historyFile)
		_ = historyFile.Close()
	}
}

func (c *CommandLine) printHistory() {
	var buf bytes.Buffer
	_, _ = c.line.WriteHistory(&buf)
	fmt.Print(buf.String())
}

func (c *CommandLine) saveHistory() {
	if c.historyFilePath == "" {
		return
	}
	if historyFile, err := os.Create(c.historyFilePath); err != nil {
		fmt.Printf("ERR: error writing history file: %s\n", err)
	} else {
		_, _ = c.line.WriteHistory(historyFile)
		_ = historyFile.Close()
	}
}

func (c *CommandLine) exit() {
	// write to history file
	c.saveHistory()
	// release line resources
	_ = c.line.Close()
	c.line = nil
}

// isWhitespace returns true if the rune is a space, tab, or newline.
func isWhitespace(ch rune) bool { return ch == ' ' || ch == '\t' || ch == '\n' }

// isLetter returns true if the rune is a letter.
func isLetter(ch rune) bool { return (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') }

// isDigit returns true if the rune is a digit.
func isDigit(ch rune) bool { return (ch >= '0' && ch <= '9') }

// isIdentFirstChar returns true if the rune can be used as the first char in an unquoted identifer.
func isIdentFirstChar(ch rune) bool { return isLetter(ch) || ch == '_' }

// isIdentChar returns true if the rune can be used in an unquoted identifier.
func isNotIdentChar(ch rune) bool { return !(isLetter(ch) || isDigit(ch) || ch == '_') }
