package cnosql_test

import (
	"reflect"
	"strings"
	"testing"

	"github.com/cnosdatabase/cnosql"
)

func TestParseTree_Clone(t *testing.T) {
	// Clone the default language parse tree and add a new syntax node.
	language := cnosql.Language.Clone()
	language.Group(cnosql.CREATE).Handle(cnosql.STATS, func(p *cnosql.Parser) (cnosql.Statement, error) {
		return &cnosql.ShowStatsStatement{}, nil
	})

	// Create a parser with CREATE STATS and parse the statement.
	parser := cnosql.NewParser(strings.NewReader(`CREATE STATS`))
	stmt, err := language.Parse(parser)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if !reflect.DeepEqual(stmt, &cnosql.ShowStatsStatement{}) {
		t.Fatalf("unexpected statement returned from parser: %s", stmt)
	}

	// Recreate the parser and try parsing with the original parsing. This should fail.
	parser = cnosql.NewParser(strings.NewReader(`CREATE STATS`))
	if _, err := parser.ParseStatement(); err == nil {
		t.Fatal("expected error")
	}
}
