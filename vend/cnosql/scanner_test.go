package cnosql_test

import (
	"reflect"
	"strings"
	"testing"

	"github.com/cnosdb/cnosdb/vend/cnosql"
)

// Ensure the scanner can scan tokens correctly.
func TestScanner_Scan(t *testing.T) {
	var tests = []struct {
		s   string
		tok cnosql.Token
		lit string
		pos cnosql.Pos
	}{
		// Special tokens (EOF, ILLEGAL, WS)
		{s: ``, tok: cnosql.EOF},
		{s: `#`, tok: cnosql.ILLEGAL, lit: `#`},
		{s: ` `, tok: cnosql.WS, lit: " "},
		{s: "\t", tok: cnosql.WS, lit: "\t"},
		{s: "\n", tok: cnosql.WS, lit: "\n"},
		{s: "\r", tok: cnosql.WS, lit: "\n"},
		{s: "\r\n", tok: cnosql.WS, lit: "\n"},
		{s: "\rX", tok: cnosql.WS, lit: "\n"},
		{s: "\n\r", tok: cnosql.WS, lit: "\n\n"},
		{s: " \n\t \r\n\t", tok: cnosql.WS, lit: " \n\t \n\t"},
		{s: " foo", tok: cnosql.WS, lit: " "},

		// Numeric operators
		{s: `+`, tok: cnosql.ADD},
		{s: `-`, tok: cnosql.SUB},
		{s: `*`, tok: cnosql.MUL},
		{s: `/`, tok: cnosql.DIV},
		{s: `%`, tok: cnosql.MOD},

		// Logical operators
		{s: `AND`, tok: cnosql.AND},
		{s: `and`, tok: cnosql.AND},
		{s: `OR`, tok: cnosql.OR},
		{s: `or`, tok: cnosql.OR},

		{s: `=`, tok: cnosql.EQ},
		{s: `<>`, tok: cnosql.NEQ},
		{s: `! `, tok: cnosql.ILLEGAL, lit: "!"},
		{s: `<`, tok: cnosql.LT},
		{s: `<=`, tok: cnosql.LTE},
		{s: `>`, tok: cnosql.GT},
		{s: `>=`, tok: cnosql.GTE},

		// Misc tokens
		{s: `(`, tok: cnosql.LPAREN},
		{s: `)`, tok: cnosql.RPAREN},
		{s: `,`, tok: cnosql.COMMA},
		{s: `;`, tok: cnosql.SEMICOLON},
		{s: `.`, tok: cnosql.DOT},
		{s: `=~`, tok: cnosql.EQREGEX},
		{s: `!~`, tok: cnosql.NEQREGEX},
		{s: `:`, tok: cnosql.COLON},
		{s: `::`, tok: cnosql.DOUBLECOLON},

		// Identifiers
		{s: `foo`, tok: cnosql.IDENT, lit: `foo`},
		{s: `_foo`, tok: cnosql.IDENT, lit: `_foo`},
		{s: `Zx12_3U_-`, tok: cnosql.IDENT, lit: `Zx12_3U_`},
		{s: `"foo"`, tok: cnosql.IDENT, lit: `foo`},
		{s: `"foo\\bar"`, tok: cnosql.IDENT, lit: `foo\bar`},
		{s: `"foo\bar"`, tok: cnosql.BADESCAPE, lit: `\b`, pos: cnosql.Pos{Line: 0, Char: 5}},
		{s: `"foo\"bar\""`, tok: cnosql.IDENT, lit: `foo"bar"`},
		{s: `test"`, tok: cnosql.BADSTRING, lit: "", pos: cnosql.Pos{Line: 0, Char: 3}},
		{s: `"test`, tok: cnosql.BADSTRING, lit: `test`},
		{s: `$host`, tok: cnosql.BOUNDPARAM, lit: `$host`},
		{s: `$"host param"`, tok: cnosql.BOUNDPARAM, lit: `$host param`},

		{s: `true`, tok: cnosql.TRUE},
		{s: `false`, tok: cnosql.FALSE},

		// Strings
		{s: `'testing 123!'`, tok: cnosql.STRING, lit: `testing 123!`},
		{s: `'foo\nbar'`, tok: cnosql.STRING, lit: "foo\nbar"},
		{s: `'foo\\bar'`, tok: cnosql.STRING, lit: "foo\\bar"},
		{s: `'test`, tok: cnosql.BADSTRING, lit: `test`},
		{s: "'test\nfoo", tok: cnosql.BADSTRING, lit: `test`},
		{s: `'test\g'`, tok: cnosql.BADESCAPE, lit: `\g`, pos: cnosql.Pos{Line: 0, Char: 6}},

		// Numbers
		{s: `100`, tok: cnosql.INTEGER, lit: `100`},
		{s: `100.23`, tok: cnosql.NUMBER, lit: `100.23`},
		{s: `.23`, tok: cnosql.NUMBER, lit: `.23`},
		//{s: `.`, tok: cnosql.ILLEGAL, lit: `.`},
		{s: `10.3s`, tok: cnosql.NUMBER, lit: `10.3`},

		// Durations
		{s: `10u`, tok: cnosql.DURATIONVAL, lit: `10u`},
		{s: `10µ`, tok: cnosql.DURATIONVAL, lit: `10µ`},
		{s: `10ms`, tok: cnosql.DURATIONVAL, lit: `10ms`},
		{s: `1s`, tok: cnosql.DURATIONVAL, lit: `1s`},
		{s: `10m`, tok: cnosql.DURATIONVAL, lit: `10m`},
		{s: `10h`, tok: cnosql.DURATIONVAL, lit: `10h`},
		{s: `10d`, tok: cnosql.DURATIONVAL, lit: `10d`},
		{s: `10w`, tok: cnosql.DURATIONVAL, lit: `10w`},
		{s: `10x`, tok: cnosql.DURATIONVAL, lit: `10x`}, // non-duration unit, but scanned as a duration value

		// Keywords
		{s: `ALL`, tok: cnosql.ALL},
		{s: `ALTER`, tok: cnosql.ALTER},
		{s: `AS`, tok: cnosql.AS},
		{s: `ASC`, tok: cnosql.ASC},
		{s: `BEGIN`, tok: cnosql.BEGIN},
		{s: `BY`, tok: cnosql.BY},
		{s: `CREATE`, tok: cnosql.CREATE},
		{s: `CONTINUOUS`, tok: cnosql.CONTINUOUS},
		{s: `DATABASE`, tok: cnosql.DATABASE},
		{s: `DATABASES`, tok: cnosql.DATABASES},
		{s: `DEFAULT`, tok: cnosql.DEFAULT},
		{s: `DELETE`, tok: cnosql.DELETE},
		{s: `DESC`, tok: cnosql.DESC},
		{s: `DROP`, tok: cnosql.DROP},
		{s: `DURATION`, tok: cnosql.DURATION},
		{s: `END`, tok: cnosql.END},
		{s: `EVERY`, tok: cnosql.EVERY},
		{s: `EXPLAIN`, tok: cnosql.EXPLAIN},
		{s: `FIELD`, tok: cnosql.FIELD},
		{s: `FROM`, tok: cnosql.FROM},
		{s: `GRANT`, tok: cnosql.GRANT},
		{s: `GROUP`, tok: cnosql.GROUP},
		{s: `GROUPS`, tok: cnosql.GROUPS},
		{s: `INSERT`, tok: cnosql.INSERT},
		{s: `INTO`, tok: cnosql.INTO},
		{s: `KEY`, tok: cnosql.KEY},
		{s: `KEYS`, tok: cnosql.KEYS},
		{s: `KILL`, tok: cnosql.KILL},
		{s: `LIMIT`, tok: cnosql.LIMIT},
		{s: `SHOW`, tok: cnosql.SHOW},
		{s: `SHARD`, tok: cnosql.SHARD},
		{s: `SHARDS`, tok: cnosql.SHARDS},
		{s: `MEASUREMENT`, tok: cnosql.MEASUREMENT},
		{s: `MEASUREMENTS`, tok: cnosql.MEASUREMENTS},
		{s: `OFFSET`, tok: cnosql.OFFSET},
		{s: `ON`, tok: cnosql.ON},
		{s: `ORDER`, tok: cnosql.ORDER},
		{s: `PASSWORD`, tok: cnosql.PASSWORD},
		{s: `POLICY`, tok: cnosql.POLICY},
		{s: `POLICIES`, tok: cnosql.POLICIES},
		{s: `PRIVILEGES`, tok: cnosql.PRIVILEGES},
		{s: `QUERIES`, tok: cnosql.QUERIES},
		{s: `QUERY`, tok: cnosql.QUERY},
		{s: `READ`, tok: cnosql.READ},
		{s: `REPLICATION`, tok: cnosql.REPLICATION},
		{s: `RESAMPLE`, tok: cnosql.RESAMPLE},
		{s: `RETENTION`, tok: cnosql.RETENTION},
		{s: `REVOKE`, tok: cnosql.REVOKE},
		{s: `SELECT`, tok: cnosql.SELECT},
		{s: `SERIES`, tok: cnosql.SERIES},
		{s: `TAG`, tok: cnosql.TAG},
		{s: `TO`, tok: cnosql.TO},
		{s: `USER`, tok: cnosql.USER},
		{s: `USERS`, tok: cnosql.USERS},
		{s: `VALUES`, tok: cnosql.VALUES},
		{s: `WHERE`, tok: cnosql.WHERE},
		{s: `WITH`, tok: cnosql.WITH},
		{s: `WRITE`, tok: cnosql.WRITE},
		{s: `explain`, tok: cnosql.EXPLAIN}, // case insensitive
		{s: `seLECT`, tok: cnosql.SELECT},   // case insensitive
	}

	for i, tt := range tests {
		s := cnosql.NewScanner(strings.NewReader(tt.s))
		tok, pos, lit := s.Scan()
		if tt.tok != tok {
			t.Errorf("%d. %q token mismatch: exp=%q got=%q <%q>", i, tt.s, tt.tok, tok, lit)
		} else if tt.pos.Line != pos.Line || tt.pos.Char != pos.Char {
			t.Errorf("%d. %q pos mismatch: exp=%#v got=%#v", i, tt.s, tt.pos, pos)
		} else if tt.lit != lit {
			t.Errorf("%d. %q literal mismatch: exp=%q got=%q", i, tt.s, tt.lit, lit)
		}
	}
}

// Ensure the scanner can scan a series of tokens correctly.
func TestScanner_Scan_Multi(t *testing.T) {
	type result struct {
		tok cnosql.Token
		pos cnosql.Pos
		lit string
	}
	exp := []result{
		{tok: cnosql.SELECT, pos: cnosql.Pos{Line: 0, Char: 0}, lit: ""},
		{tok: cnosql.WS, pos: cnosql.Pos{Line: 0, Char: 6}, lit: " "},
		{tok: cnosql.IDENT, pos: cnosql.Pos{Line: 0, Char: 7}, lit: "value"},
		{tok: cnosql.WS, pos: cnosql.Pos{Line: 0, Char: 12}, lit: " "},
		{tok: cnosql.FROM, pos: cnosql.Pos{Line: 0, Char: 13}, lit: ""},
		{tok: cnosql.WS, pos: cnosql.Pos{Line: 0, Char: 17}, lit: " "},
		{tok: cnosql.IDENT, pos: cnosql.Pos{Line: 0, Char: 18}, lit: "myseries"},
		{tok: cnosql.WS, pos: cnosql.Pos{Line: 0, Char: 26}, lit: " "},
		{tok: cnosql.WHERE, pos: cnosql.Pos{Line: 0, Char: 27}, lit: ""},
		{tok: cnosql.WS, pos: cnosql.Pos{Line: 0, Char: 32}, lit: " "},
		{tok: cnosql.IDENT, pos: cnosql.Pos{Line: 0, Char: 33}, lit: "a"},
		{tok: cnosql.WS, pos: cnosql.Pos{Line: 0, Char: 34}, lit: " "},
		{tok: cnosql.EQ, pos: cnosql.Pos{Line: 0, Char: 35}, lit: ""},
		{tok: cnosql.WS, pos: cnosql.Pos{Line: 0, Char: 36}, lit: " "},
		{tok: cnosql.STRING, pos: cnosql.Pos{Line: 0, Char: 36}, lit: "b"},
		{tok: cnosql.EOF, pos: cnosql.Pos{Line: 0, Char: 40}, lit: ""},
	}

	// Create a scanner.
	v := `SELECT value from myseries WHERE a = 'b'`
	s := cnosql.NewScanner(strings.NewReader(v))

	// Continually scan until we reach the end.
	var act []result
	for {
		tok, pos, lit := s.Scan()
		act = append(act, result{tok, pos, lit})
		if tok == cnosql.EOF {
			break
		}
	}

	// Verify the token counts match.
	if len(exp) != len(act) {
		t.Fatalf("token count mismatch: exp=%d, got=%d", len(exp), len(act))
	}

	// Verify each token matches.
	for i := range exp {
		if !reflect.DeepEqual(exp[i], act[i]) {
			t.Fatalf("%d. token mismatch:\n\nexp=%#v\n\ngot=%#v", i, exp[i], act[i])
		}
	}
}

// Ensure the library can correctly scan strings.
func TestScanString(t *testing.T) {
	var tests = []struct {
		in  string
		out string
		err string
	}{
		{in: `""`, out: ``},
		{in: `"foo bar"`, out: `foo bar`},
		{in: `'foo bar'`, out: `foo bar`},
		{in: `"foo\nbar"`, out: "foo\nbar"},
		{in: `"foo\\bar"`, out: `foo\bar`},
		{in: `"foo\"bar"`, out: `foo"bar`},
		{in: `'foo\'bar'`, out: `foo'bar`},

		{in: `"foo` + "\n", out: `foo`, err: "bad string"}, // newline in string
		{in: `"foo`, out: `foo`, err: "bad string"},        // unclosed quotes
		{in: `"foo\xbar"`, out: `\x`, err: "bad escape"},   // invalid escape
	}

	for i, tt := range tests {
		out, err := cnosql.ScanString(strings.NewReader(tt.in))
		if tt.err != errstring(err) {
			t.Errorf("%d. %s: error: exp=%s, got=%s", i, tt.in, tt.err, err)
		} else if tt.out != out {
			t.Errorf("%d. %s: out: exp=%s, got=%s", i, tt.in, tt.out, out)
		}
	}
}

// Test scanning regex
func TestScanRegex(t *testing.T) {
	var tests = []struct {
		in  string
		tok cnosql.Token
		lit string
		err string
	}{
		{in: `/^payments\./`, tok: cnosql.REGEX, lit: `^payments\.`},
		{in: `/foo\/bar/`, tok: cnosql.REGEX, lit: `foo/bar`},
		{in: `/foo\\/bar/`, tok: cnosql.REGEX, lit: `foo\/bar`},
		{in: `/foo\\bar/`, tok: cnosql.REGEX, lit: `foo\\bar`},
		{in: `/http\:\/\/www\.example\.com/`, tok: cnosql.REGEX, lit: `http\://www\.example\.com`},
	}

	for i, tt := range tests {
		s := cnosql.NewScanner(strings.NewReader(tt.in))
		tok, _, lit := s.ScanRegex()
		if tok != tt.tok {
			t.Errorf("%d. %s: error:\n\texp=%s\n\tgot=%s\n", i, tt.in, tt.tok.String(), tok.String())
		}
		if lit != tt.lit {
			t.Errorf("%d. %s: error:\n\texp=%s\n\tgot=%s\n", i, tt.in, tt.lit, lit)
		}
	}
}
