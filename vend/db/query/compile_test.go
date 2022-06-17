package query_test

import (
	"context"
	"github.com/cnosdb/cnosdb/vend/cnosql"
	"github.com/cnosdb/cnosdb/vend/db/query"
	"testing"
	"time"
)

func TestCompile_Success(t *testing.T) {
	for _, tt := range []string{
		`SELECT time, value FROM air`,
		`SELECT value FROM air`,
		`SELECT value, host FROM air`,
		`SELECT * FROM air`,
		`SELECT time, * FROM air`,
		`SELECT value, * FROM air`,
		`SELECT max(value) FROM air`,
		`SELECT max(value), host FROM air`,
		`SELECT max(value), * FROM air`,
		`SELECT max(*) FROM air`,
		`SELECT max(/val/) FROM air`,
		`SELECT min(value) FROM air`,
		`SELECT min(value), host FROM air`,
		`SELECT min(value), * FROM air`,
		`SELECT min(*) FROM air`,
		`SELECT min(/val/) FROM air`,
		`SELECT first(value) FROM air`,
		`SELECT first(value), host FROM air`,
		`SELECT first(value), * FROM air`,
		`SELECT first(*) FROM air`,
		`SELECT first(/val/) FROM air`,
		`SELECT last(value) FROM air`,
		`SELECT last(value), host FROM air`,
		`SELECT last(value), * FROM air`,
		`SELECT last(*) FROM air`,
		`SELECT last(/val/) FROM air`,
		`SELECT count(value) FROM air`,
		`SELECT count(distinct(value)) FROM air`,
		`SELECT count(distinct value) FROM air`,
		`SELECT count(*) FROM air`,
		`SELECT count(/val/) FROM air`,
		`SELECT mean(value) FROM air`,
		`SELECT mean(*) FROM air`,
		`SELECT mean(/val/) FROM air`,
		`SELECT min(value), max(value) FROM air`,
		`SELECT min(*), max(*) FROM air`,
		`SELECT min(/val/), max(/val/) FROM air`,
		`SELECT first(value), last(value) FROM air`,
		`SELECT first(*), last(*) FROM air`,
		`SELECT first(/val/), last(/val/) FROM air`,
		`SELECT count(value) FROM air WHERE time >= now() - 1h GROUP BY time(10m)`,
		`SELECT distinct value FROM air`,
		`SELECT distinct(value) FROM air`,
		`SELECT value / total FROM air`,
		`SELECT min(value) / total FROM air`,
		`SELECT max(value) / total FROM air`,
		`SELECT top(value, 1) FROM air`,
		`SELECT top(value, host, 1) FROM air`,
		`SELECT top(value, 1), host FROM air`,
		`SELECT min(top) FROM (SELECT top(value, host, 1) FROM air) GROUP BY region`,
		`SELECT bottom(value, 1) FROM air`,
		`SELECT bottom(value, host, 1) FROM air`,
		`SELECT bottom(value, 1), host FROM air`,
		`SELECT max(bottom) FROM (SELECT bottom(value, host, 1) FROM air) GROUP BY region`,
		`SELECT percentile(value, 75) FROM air`,
		`SELECT percentile(value, 75.0) FROM air`,
		`SELECT sample(value, 2) FROM air`,
		`SELECT sample(*, 2) FROM air`,
		`SELECT sample(/val/, 2) FROM air`,
		`SELECT elapsed(value) FROM air`,
		`SELECT elapsed(value, 10s) FROM air`,
		`SELECT integral(value) FROM air`,
		`SELECT integral(value, 10s) FROM air`,
		`SELECT max(value) FROM air WHERE time >= now() - 1m GROUP BY time(10s, 5s)`,
		`SELECT max(value) FROM air WHERE time >= now() - 1m GROUP BY time(10s, '2000-01-01T00:00:05Z')`,
		`SELECT max(value) FROM air WHERE time >= now() - 1m GROUP BY time(10s, now())`,
		`SELECT max(mean) FROM (SELECT mean(value) FROM air GROUP BY host)`,
		`SELECT max(derivative) FROM (SELECT derivative(mean(value)) FROM air) WHERE time >= now() - 1m GROUP BY time(10s)`,
		`SELECT max(value) FROM (SELECT value + total FROM air) WHERE time >= now() - 1m GROUP BY time(10s)`,
		`SELECT value FROM air WHERE time >= '2000-01-01T00:00:00Z' AND time <= '2000-01-01T01:00:00Z'`,
		`SELECT value FROM (SELECT value FROM air) ORDER BY time DESC`,
		`SELECT count(distinct(value)), max(value) FROM air`,
		`SELECT derivative(distinct(value)), difference(distinct(value)) FROM air WHERE time >= now() - 1m GROUP BY time(5s)`,
		`SELECT moving_average(distinct(value), 3) FROM air WHERE time >= now() - 5m GROUP BY time(1m)`,
		`SELECT elapsed(distinct(value)) FROM air WHERE time >= now() - 5m GROUP BY time(1m)`,
		`SELECT cumulative_sum(distinct(value)) FROM air WHERE time >= now() - 5m GROUP BY time(1m)`,
		`SELECT last(value) / (1 - 0) FROM air`,
		`SELECT abs(value) FROM air`,
		`SELECT sin(value) FROM air`,
		`SELECT cos(value) FROM air`,
		`SELECT tan(value) FROM air`,
		`SELECT asin(value) FROM air`,
		`SELECT acos(value) FROM air`,
		`SELECT atan(value) FROM air`,
		`SELECT sqrt(value) FROM air`,
		`SELECT pow(value, 2) FROM air`,
		`SELECT pow(value, 3.14) FROM air`,
		`SELECT pow(2, value) FROM air`,
		`SELECT pow(3.14, value) FROM air`,
		`SELECT exp(value) FROM air`,
		`SELECT atan2(value, 0.1) FROM air`,
		`SELECT atan2(0.2, value) FROM air`,
		`SELECT atan2(value, 1) FROM air`,
		`SELECT atan2(2, value) FROM air`,
		`SELECT ln(value) FROM air`,
		`SELECT log(value, 2) FROM air`,
		`SELECT log2(value) FROM air`,
		`SELECT log10(value) FROM air`,
		`SELECT sin(value) - sin(1.3) FROM air`,
		`SELECT value FROM air WHERE sin(value) > 0.5`,
		`SELECT sum("out")/sum("in") FROM (SELECT derivative("out") AS "out", derivative("in") AS "in" FROM "m0" WHERE time >= now() - 5m GROUP BY "index") GROUP BY time(1m) fill(none)`,
	} {
		t.Run(tt, func(t *testing.T) {
			stmt, err := cnosql.ParseStatement(tt)
			if err != nil {
				t.Fatalf("unexpected error: %s", err)
			}
			s := stmt.(*cnosql.SelectStatement)

			opt := query.CompileOptions{}
			if _, err := query.Compile(s, opt); err != nil {
				t.Errorf("unexpected error: %s", err)
			}
		})
	}
}

func TestCompile_Failures(t *testing.T) {
	for _, tt := range []struct {
		s   string
		err string
	}{
		{s: `SELECT time FROM air`, err: `at least 1 non-time field must be queried`},
		{s: `SELECT value, mean(value) FROM air`, err: `mixing aggregate and non-aggregate queries is not supported`},
		{s: `SELECT value, max(value), min(value) FROM air`, err: `mixing multiple selector functions with tags or fields is not supported`},
		{s: `SELECT top(value, 10), max(value) FROM air`, err: `selector function top() cannot be combined with other functions`},
		{s: `SELECT bottom(value, 10), max(value) FROM air`, err: `selector function bottom() cannot be combined with other functions`},
		{s: `SELECT count() FROM air`, err: `invalid number of arguments for count, expected 1, got 0`},
		{s: `SELECT count(value, host) FROM air`, err: `invalid number of arguments for count, expected 1, got 2`},
		{s: `SELECT min() FROM air`, err: `invalid number of arguments for min, expected 1, got 0`},
		{s: `SELECT min(value, host) FROM air`, err: `invalid number of arguments for min, expected 1, got 2`},
		{s: `SELECT max() FROM air`, err: `invalid number of arguments for max, expected 1, got 0`},
		{s: `SELECT max(value, host) FROM air`, err: `invalid number of arguments for max, expected 1, got 2`},
		{s: `SELECT sum() FROM air`, err: `invalid number of arguments for sum, expected 1, got 0`},
		{s: `SELECT sum(value, host) FROM air`, err: `invalid number of arguments for sum, expected 1, got 2`},
		{s: `SELECT first() FROM air`, err: `invalid number of arguments for first, expected 1, got 0`},
		{s: `SELECT first(value, host) FROM air`, err: `invalid number of arguments for first, expected 1, got 2`},
		{s: `SELECT last() FROM air`, err: `invalid number of arguments for last, expected 1, got 0`},
		{s: `SELECT last(value, host) FROM air`, err: `invalid number of arguments for last, expected 1, got 2`},
		{s: `SELECT mean() FROM air`, err: `invalid number of arguments for mean, expected 1, got 0`},
		{s: `SELECT mean(value, host) FROM air`, err: `invalid number of arguments for mean, expected 1, got 2`},
		{s: `SELECT distinct(value), max(value) FROM air`, err: `aggregate function distinct() cannot be combined with other functions or fields`},
		{s: `SELECT count(distinct()) FROM air`, err: `distinct function requires at least one argument`},
		{s: `SELECT count(distinct(value, host)) FROM air`, err: `distinct function can only have one argument`},
		{s: `SELECT count(distinct(2)) FROM air`, err: `expected field argument in distinct()`},
		{s: `SELECT value FROM air GROUP BY now()`, err: `only time() calls allowed in dimensions`},
		{s: `SELECT value FROM air GROUP BY time()`, err: `time dimension expected 1 or 2 arguments`},
		{s: `SELECT value FROM air GROUP BY time(5m, 30s, 1ms)`, err: `time dimension expected 1 or 2 arguments`},
		{s: `SELECT value FROM air GROUP BY time('unexpected')`, err: `time dimension must have duration argument`},
		{s: `SELECT value FROM air GROUP BY time(5m), time(1m)`, err: `multiple time dimensions not allowed`},
		{s: `SELECT value FROM air GROUP BY time(5m, unexpected())`, err: `time dimension offset function must be now()`},
		{s: `SELECT value FROM air GROUP BY time(5m, now(1m))`, err: `time dimension offset now() function requires no arguments`},
		{s: `SELECT value FROM air GROUP BY time(5m, 'unexpected')`, err: `time dimension offset must be duration or now()`},
		{s: `SELECT value FROM air GROUP BY 'unexpected'`, err: `only time and tag dimensions allowed`},
		{s: `SELECT top(value) FROM air`, err: `invalid number of arguments for top, expected at least 2, got 1`},
		{s: `SELECT top('unexpected', 5) FROM air`, err: `expected first argument to be a field in top(), found 'unexpected'`},
		{s: `SELECT top(value, 'unexpected', 5) FROM air`, err: `only fields or tags are allowed in top(), found 'unexpected'`},
		{s: `SELECT top(value, 2.5) FROM air`, err: `expected integer as last argument in top(), found 2.500`},
		{s: `SELECT top(value, -1) FROM air`, err: `limit (-1) in top function must be at least 1`},
		{s: `SELECT top(value, 3) FROM air LIMIT 2`, err: `limit (3) in top function can not be larger than the LIMIT (2) in the select statement`},
		{s: `SELECT bottom(value) FROM air`, err: `invalid number of arguments for bottom, expected at least 2, got 1`},
		{s: `SELECT bottom('unexpected', 5) FROM air`, err: `expected first argument to be a field in bottom(), found 'unexpected'`},
		{s: `SELECT bottom(value, 'unexpected', 5) FROM air`, err: `only fields or tags are allowed in bottom(), found 'unexpected'`},
		{s: `SELECT bottom(value, 2.5) FROM air`, err: `expected integer as last argument in bottom(), found 2.500`},
		{s: `SELECT bottom(value, -1) FROM air`, err: `limit (-1) in bottom function must be at least 1`},
		{s: `SELECT bottom(value, 3) FROM air LIMIT 2`, err: `limit (3) in bottom function can not be larger than the LIMIT (2) in the select statement`},

		{s: `SELECT value FROM air WHERE value`, err: `invalid condition expression: value`},
		{s: `SELECT count(value), * FROM air`, err: `mixing aggregate and non-aggregate queries is not supported`},
		{s: `SELECT max(*), host FROM air`, err: `mixing aggregate and non-aggregate queries is not supported`},
		{s: `SELECT count(value), /ho/ FROM air`, err: `mixing aggregate and non-aggregate queries is not supported`},
		{s: `SELECT max(/val/), * FROM air`, err: `mixing aggregate and non-aggregate queries is not supported`},
		{s: `SELECT a(value) FROM air`, err: `undefined function a()`},
		{s: `SELECT count(max(value)) FROM myseries`, err: `expected field argument in count()`},
		{s: `SELECT count(distinct('value')) FROM myseries`, err: `expected field argument in distinct()`},
		{s: `SELECT distinct('value') FROM myseries`, err: `expected field argument in distinct()`},
		{s: `SELECT min(max(value)) FROM myseries`, err: `expected field argument in min()`},
		{s: `SELECT min(distinct(value)) FROM myseries`, err: `expected field argument in min()`},
		{s: `SELECT max(max(value)) FROM myseries`, err: `expected field argument in max()`},
		{s: `SELECT sum(max(value)) FROM myseries`, err: `expected field argument in sum()`},
		{s: `SELECT first(max(value)) FROM myseries`, err: `expected field argument in first()`},
		{s: `SELECT last(max(value)) FROM myseries`, err: `expected field argument in last()`},
		{s: `SELECT mean(max(value)) FROM myseries`, err: `expected field argument in mean()`},
		{s: `SELECT median(max(value)) FROM myseries`, err: `expected field argument in median()`},
		{s: `SELECT mode(max(value)) FROM myseries`, err: `expected field argument in mode()`},
		{s: `SELECT stddev(max(value)) FROM myseries`, err: `expected field argument in stddev()`},
		{s: `SELECT spread(max(value)) FROM myseries`, err: `expected field argument in spread()`},
		{s: `SELECT top() FROM myseries`, err: `invalid number of arguments for top, expected at least 2, got 0`},
		{s: `SELECT top(field1) FROM myseries`, err: `invalid number of arguments for top, expected at least 2, got 1`},
		{s: `SELECT top(field1,foo) FROM myseries`, err: `expected integer as last argument in top(), found foo`},
		{s: `SELECT top(field1,host,'server',foo) FROM myseries`, err: `expected integer as last argument in top(), found foo`},
		{s: `SELECT top(field1,5,'server',2) FROM myseries`, err: `only fields or tags are allowed in top(), found 5`},
		{s: `SELECT top(field1,max(foo),'server',2) FROM myseries`, err: `only fields or tags are allowed in top(), found max(foo)`},
		{s: `SELECT top(value, 10) + count(value) FROM myseries`, err: `selector function top() cannot be combined with other functions`},
		{s: `SELECT top(max(value), 10) FROM myseries`, err: `expected first argument to be a field in top(), found max(value)`},
		{s: `SELECT bottom() FROM myseries`, err: `invalid number of arguments for bottom, expected at least 2, got 0`},
		{s: `SELECT bottom(field1) FROM myseries`, err: `invalid number of arguments for bottom, expected at least 2, got 1`},
		{s: `SELECT bottom(field1,foo) FROM myseries`, err: `expected integer as last argument in bottom(), found foo`},
		{s: `SELECT bottom(field1,host,'server',foo) FROM myseries`, err: `expected integer as last argument in bottom(), found foo`},
		{s: `SELECT bottom(field1,5,'server',2) FROM myseries`, err: `only fields or tags are allowed in bottom(), found 5`},
		{s: `SELECT bottom(field1,max(foo),'server',2) FROM myseries`, err: `only fields or tags are allowed in bottom(), found max(foo)`},
		{s: `SELECT bottom(value, 10) + count(value) FROM myseries`, err: `selector function bottom() cannot be combined with other functions`},
		{s: `SELECT bottom(max(value), 10) FROM myseries`, err: `expected first argument to be a field in bottom(), found max(value)`},
		{s: `SELECT top(value, 10), bottom(value, 10) FROM air`, err: `selector function top() cannot be combined with other functions`},
		{s: `SELECT bottom(value, 10), top(value, 10) FROM air`, err: `selector function bottom() cannot be combined with other functions`},
		{s: `SELECT sample(value) FROM myseries`, err: `invalid number of arguments for sample, expected 2, got 1`},
		{s: `SELECT sample(value, 2, 3) FROM myseries`, err: `invalid number of arguments for sample, expected 2, got 3`},
		{s: `SELECT sample(value, 0) FROM myseries`, err: `sample window must be greater than 1, got 0`},
		{s: `SELECT sample(value, 2.5) FROM myseries`, err: `expected integer argument in sample()`},
		{s: `SELECT percentile() FROM myseries`, err: `invalid number of arguments for percentile, expected 2, got 0`},
		{s: `SELECT percentile(field1) FROM myseries`, err: `invalid number of arguments for percentile, expected 2, got 1`},
		{s: `SELECT percentile(field1, foo) FROM myseries`, err: `expected float argument in percentile()`},
		{s: `SELECT percentile(max(field1), 75) FROM myseries`, err: `expected field argument in percentile()`},
		{s: `SELECT field1 FROM foo group by time(1s)`, err: `GROUP BY requires at least one aggregate function`},
		{s: `SELECT field1 FROM foo fill(none)`, err: `fill(none) must be used with a function`},
		{s: `SELECT field1 FROM foo fill(linear)`, err: `fill(linear) must be used with a function`},
		{s: `SELECT count(value), value FROM foo`, err: `mixing aggregate and non-aggregate queries is not supported`},
		{s: `SELECT count(value) FROM foo group by time`, err: `time() is a function and expects at least one argument`},
		{s: `SELECT count(value) FROM foo group by 'time'`, err: `only time and tag dimensions allowed`},
		{s: `SELECT count(value) FROM foo where time > now() and time < now() group by time()`, err: `time dimension expected 1 or 2 arguments`},
		{s: `SELECT count(value) FROM foo where time > now() and time < now() group by time(b)`, err: `time dimension must have duration argument`},
		{s: `SELECT count(value) FROM foo where time > now() and time < now() group by time(1s), time(2s)`, err: `multiple time dimensions not allowed`},
		{s: `SELECT count(value) FROM foo where time > now() and time < now() group by time(1s, b)`, err: `time dimension offset must be duration or now()`},
		{s: `SELECT count(value) FROM foo where time > now() and time < now() group by time(1s, '5s')`, err: `time dimension offset must be duration or now()`},
		{s: `SELECT distinct(field1), sum(field1) FROM myseries`, err: `aggregate function distinct() cannot be combined with other functions or fields`},
		{s: `SELECT distinct(field1), field2 FROM myseries`, err: `aggregate function distinct() cannot be combined with other functions or fields`},
		{s: `SELECT distinct(field1, field2) FROM myseries`, err: `distinct function can only have one argument`},
		{s: `SELECT distinct() FROM myseries`, err: `distinct function requires at least one argument`},
		{s: `SELECT distinct field1, field2 FROM myseries`, err: `aggregate function distinct() cannot be combined with other functions or fields`},
		{s: `SELECT count(distinct field1, field2) FROM myseries`, err: `invalid number of arguments for count, expected 1, got 2`},
		{s: `select count(distinct(too, many, arguments)) from myseries`, err: `distinct function can only have one argument`},
		{s: `select count() from myseries`, err: `invalid number of arguments for count, expected 1, got 0`},
		{s: `SELECT derivative(field1), field1 FROM myseries`, err: `mixing aggregate and non-aggregate queries is not supported`},
		{s: `select derivative() from myseries`, err: `invalid number of arguments for derivative, expected at least 1 but no more than 2, got 0`},
		{s: `select derivative(mean(value), 1h, 3) from myseries`, err: `invalid number of arguments for derivative, expected at least 1 but no more than 2, got 3`},
		{s: `SELECT derivative(value) FROM myseries group by time(1h)`, err: `aggregate function required inside the call to derivative`},
		{s: `SELECT derivative(top(value)) FROM myseries where time < now() and time > now() - 1d group by time(1h)`, err: `invalid number of arguments for top, expected at least 2, got 1`},
		{s: `SELECT derivative(bottom(value)) FROM myseries where time < now() and time > now() - 1d group by time(1h)`, err: `invalid number of arguments for bottom, expected at least 2, got 1`},
		{s: `SELECT derivative(max()) FROM myseries where time < now() and time > now() - 1d group by time(1h)`, err: `invalid number of arguments for max, expected 1, got 0`},
		{s: `SELECT derivative(percentile(value)) FROM myseries where time < now() and time > now() - 1d group by time(1h)`, err: `invalid number of arguments for percentile, expected 2, got 1`},
		{s: `SELECT derivative(mean(value), 1h) FROM myseries where time < now() and time > now() - 1d`, err: `derivative aggregate requires a GROUP BY interval`},
		{s: `SELECT derivative(value, -2h) FROM myseries`, err: `duration argument must be positive, got -2h`},
		{s: `SELECT derivative(value, 10) FROM myseries`, err: `second argument to derivative must be a duration, got *cnosql.IntegerLiteral`},
		{s: `SELECT derivative(f, true) FROM myseries`, err: `second argument to derivative must be a duration, got *cnosql.BooleanLiteral`},
		{s: `SELECT non_negative_derivative(field1), field1 FROM myseries`, err: `mixing aggregate and non-aggregate queries is not supported`},
		{s: `select non_negative_derivative() from myseries`, err: `invalid number of arguments for non_negative_derivative, expected at least 1 but no more than 2, got 0`},
		{s: `select non_negative_derivative(mean(value), 1h, 3) from myseries`, err: `invalid number of arguments for non_negative_derivative, expected at least 1 but no more than 2, got 3`},
		{s: `SELECT non_negative_derivative(value) FROM myseries group by time(1h)`, err: `aggregate function required inside the call to non_negative_derivative`},
		{s: `SELECT non_negative_derivative(top(value)) FROM myseries where time < now() and time > now() - 1d group by time(1h)`, err: `invalid number of arguments for top, expected at least 2, got 1`},
		{s: `SELECT non_negative_derivative(bottom(value)) FROM myseries where time < now() and time > now() - 1d group by time(1h)`, err: `invalid number of arguments for bottom, expected at least 2, got 1`},
		{s: `SELECT non_negative_derivative(max()) FROM myseries where time < now() and time > now() - 1d group by time(1h)`, err: `invalid number of arguments for max, expected 1, got 0`},
		{s: `SELECT non_negative_derivative(mean(value), 1h) FROM myseries where time < now() and time > now() - 1d`, err: `non_negative_derivative aggregate requires a GROUP BY interval`},
		{s: `SELECT non_negative_derivative(percentile(value)) FROM myseries where time < now() and time > now() - 1d group by time(1h)`, err: `invalid number of arguments for percentile, expected 2, got 1`},
		{s: `SELECT non_negative_derivative(value, -2h) FROM myseries`, err: `duration argument must be positive, got -2h`},
		{s: `SELECT non_negative_derivative(value, 10) FROM myseries`, err: `second argument to non_negative_derivative must be a duration, got *cnosql.IntegerLiteral`},
		{s: `SELECT difference(field1), field1 FROM myseries`, err: `mixing aggregate and non-aggregate queries is not supported`},
		{s: `SELECT difference() from myseries`, err: `invalid number of arguments for difference, expected 1, got 0`},
		{s: `SELECT difference(value) FROM myseries group by time(1h)`, err: `aggregate function required inside the call to difference`},
		{s: `SELECT difference(top(value)) FROM myseries where time < now() and time > now() - 1d group by time(1h)`, err: `invalid number of arguments for top, expected at least 2, got 1`},
		{s: `SELECT difference(bottom(value)) FROM myseries where time < now() and time > now() - 1d group by time(1h)`, err: `invalid number of arguments for bottom, expected at least 2, got 1`},
		{s: `SELECT difference(max()) FROM myseries where time < now() and time > now() - 1d group by time(1h)`, err: `invalid number of arguments for max, expected 1, got 0`},
		{s: `SELECT difference(percentile(value)) FROM myseries where time < now() and time > now() - 1d group by time(1h)`, err: `invalid number of arguments for percentile, expected 2, got 1`},
		{s: `SELECT difference(mean(value)) FROM myseries where time < now() and time > now() - 1d`, err: `difference aggregate requires a GROUP BY interval`},
		{s: `SELECT non_negative_difference(field1), field1 FROM myseries`, err: `mixing aggregate and non-aggregate queries is not supported`},
		{s: `SELECT non_negative_difference() from myseries`, err: `invalid number of arguments for non_negative_difference, expected 1, got 0`},
		{s: `SELECT non_negative_difference(value) FROM myseries group by time(1h)`, err: `aggregate function required inside the call to non_negative_difference`},
		{s: `SELECT non_negative_difference(top(value)) FROM myseries where time < now() and time > now() - 1d group by time(1h)`, err: `invalid number of arguments for top, expected at least 2, got 1`},
		{s: `SELECT non_negative_difference(bottom(value)) FROM myseries where time < now() and time > now() - 1d group by time(1h)`, err: `invalid number of arguments for bottom, expected at least 2, got 1`},
		{s: `SELECT non_negative_difference(max()) FROM myseries where time < now() and time > now() - 1d group by time(1h)`, err: `invalid number of arguments for max, expected 1, got 0`},
		{s: `SELECT non_negative_difference(percentile(value)) FROM myseries where time < now() and time > now() - 1d group by time(1h)`, err: `invalid number of arguments for percentile, expected 2, got 1`},
		{s: `SELECT non_negative_difference(mean(value)) FROM myseries where time < now() and time > now() - 1d`, err: `non_negative_difference aggregate requires a GROUP BY interval`},
		{s: `SELECT elapsed() FROM myseries`, err: `invalid number of arguments for elapsed, expected at least 1 but no more than 2, got 0`},
		{s: `SELECT elapsed(value) FROM myseries group by time(1h)`, err: `aggregate function required inside the call to elapsed`},
		{s: `SELECT elapsed(value, 1s, host) FROM myseries`, err: `invalid number of arguments for elapsed, expected at least 1 but no more than 2, got 3`},
		{s: `SELECT elapsed(value, 0s) FROM myseries`, err: `duration argument must be positive, got 0s`},
		{s: `SELECT elapsed(value, -10s) FROM myseries`, err: `duration argument must be positive, got -10s`},
		{s: `SELECT elapsed(value, 10) FROM myseries`, err: `second argument to elapsed must be a duration, got *cnosql.IntegerLiteral`},
		{s: `SELECT elapsed(top(value)) FROM myseries where time < now() and time > now() - 1d group by time(1h)`, err: `invalid number of arguments for top, expected at least 2, got 1`},
		{s: `SELECT elapsed(bottom(value)) FROM myseries where time < now() and time > now() - 1d group by time(1h)`, err: `invalid number of arguments for bottom, expected at least 2, got 1`},
		{s: `SELECT elapsed(max()) FROM myseries where time < now() and time > now() - 1d group by time(1h)`, err: `invalid number of arguments for max, expected 1, got 0`},
		{s: `SELECT elapsed(percentile(value)) FROM myseries where time < now() and time > now() - 1d group by time(1h)`, err: `invalid number of arguments for percentile, expected 2, got 1`},
		{s: `SELECT elapsed(mean(value)) FROM myseries where time < now() and time > now() - 1d`, err: `elapsed aggregate requires a GROUP BY interval`},
		{s: `SELECT moving_average(field1, 2), field1 FROM myseries`, err: `mixing aggregate and non-aggregate queries is not supported`},
		{s: `SELECT moving_average(field1, 1), field1 FROM myseries`, err: `moving_average window must be greater than 1, got 1`},
		{s: `SELECT moving_average(field1, 0), field1 FROM myseries`, err: `moving_average window must be greater than 1, got 0`},
		{s: `SELECT moving_average(field1, -1), field1 FROM myseries`, err: `moving_average window must be greater than 1, got -1`},
		{s: `SELECT moving_average(field1, 2.0), field1 FROM myseries`, err: `second argument for moving_average must be an integer, got *cnosql.NumberLiteral`},
		{s: `SELECT moving_average() from myseries`, err: `invalid number of arguments for moving_average, expected 2, got 0`},
		{s: `SELECT moving_average(value) FROM myseries`, err: `invalid number of arguments for moving_average, expected 2, got 1`},
		{s: `SELECT moving_average(value, 2) FROM myseries group by time(1h)`, err: `aggregate function required inside the call to moving_average`},
		{s: `SELECT moving_average(top(value), 2) FROM myseries where time < now() and time > now() - 1d group by time(1h)`, err: `invalid number of arguments for top, expected at least 2, got 1`},
		{s: `SELECT moving_average(bottom(value), 2) FROM myseries where time < now() and time > now() - 1d group by time(1h)`, err: `invalid number of arguments for bottom, expected at least 2, got 1`},
		{s: `SELECT moving_average(max(), 2) FROM myseries where time < now() and time > now() - 1d group by time(1h)`, err: `invalid number of arguments for max, expected 1, got 0`},
		{s: `SELECT moving_average(percentile(value), 2) FROM myseries where time < now() and time > now() - 1d group by time(1h)`, err: `invalid number of arguments for percentile, expected 2, got 1`},
		{s: `SELECT moving_average(mean(value), 2) FROM myseries where time < now() and time > now() - 1d`, err: `moving_average aggregate requires a GROUP BY interval`},
		{s: `SELECT cumulative_sum(field1), field1 FROM myseries`, err: `mixing aggregate and non-aggregate queries is not supported`},
		{s: `SELECT cumulative_sum() from myseries`, err: `invalid number of arguments for cumulative_sum, expected 1, got 0`},
		{s: `SELECT cumulative_sum(value) FROM myseries group by time(1h)`, err: `aggregate function required inside the call to cumulative_sum`},
		{s: `SELECT cumulative_sum(top(value)) FROM myseries where time < now() and time > now() - 1d group by time(1h)`, err: `invalid number of arguments for top, expected at least 2, got 1`},
		{s: `SELECT cumulative_sum(bottom(value)) FROM myseries where time < now() and time > now() - 1d group by time(1h)`, err: `invalid number of arguments for bottom, expected at least 2, got 1`},
		{s: `SELECT cumulative_sum(max()) FROM myseries where time < now() and time > now() - 1d group by time(1h)`, err: `invalid number of arguments for max, expected 1, got 0`},
		{s: `SELECT cumulative_sum(percentile(value)) FROM myseries where time < now() and time > now() - 1d group by time(1h)`, err: `invalid number of arguments for percentile, expected 2, got 1`},
		{s: `SELECT cumulative_sum(mean(value)) FROM myseries where time < now() and time > now() - 1d`, err: `cumulative_sum aggregate requires a GROUP BY interval`},
		{s: `SELECT integral() FROM myseries`, err: `invalid number of arguments for integral, expected at least 1 but no more than 2, got 0`},
		{s: `SELECT integral(value, 10s, host) FROM myseries`, err: `invalid number of arguments for integral, expected at least 1 but no more than 2, got 3`},
		{s: `SELECT integral(value, -10s) FROM myseries`, err: `duration argument must be positive, got -10s`},
		{s: `SELECT integral(value, 10) FROM myseries`, err: `second argument must be a duration`},
		{s: `SELECT holt_winters(value) FROM myseries where time < now() and time > now() - 1d`, err: `invalid number of arguments for holt_winters, expected 3, got 1`},
		{s: `SELECT holt_winters(value, 10, 2) FROM myseries where time < now() and time > now() - 1d`, err: `must use aggregate function with holt_winters`},
		{s: `SELECT holt_winters(min(value), 10, 2) FROM myseries where time < now() and time > now() - 1d`, err: `holt_winters aggregate requires a GROUP BY interval`},
		{s: `SELECT holt_winters(min(value), 0, 2) FROM myseries where time < now() and time > now() - 1d GROUP BY time(1d)`, err: `second arg to holt_winters must be greater than 0, got 0`},
		{s: `SELECT holt_winters(min(value), false, 2) FROM myseries where time < now() and time > now() - 1d GROUP BY time(1d)`, err: `expected integer argument as second arg in holt_winters`},
		{s: `SELECT holt_winters(min(value), 10, 'string') FROM myseries where time < now() and time > now() - 1d GROUP BY time(1d)`, err: `expected integer argument as third arg in holt_winters`},
		{s: `SELECT holt_winters(min(value), 10, -1) FROM myseries where time < now() and time > now() - 1d GROUP BY time(1d)`, err: `third arg to holt_winters cannot be negative, got -1`},
		{s: `SELECT holt_winters_with_fit(value) FROM myseries where time < now() and time > now() - 1d`, err: `invalid number of arguments for holt_winters_with_fit, expected 3, got 1`},
		{s: `SELECT holt_winters_with_fit(value, 10, 2) FROM myseries where time < now() and time > now() - 1d`, err: `must use aggregate function with holt_winters_with_fit`},
		{s: `SELECT holt_winters_with_fit(min(value), 10, 2) FROM myseries where time < now() and time > now() - 1d`, err: `holt_winters_with_fit aggregate requires a GROUP BY interval`},
		{s: `SELECT holt_winters_with_fit(min(value), 0, 2) FROM myseries where time < now() and time > now() - 1d GROUP BY time(1d)`, err: `second arg to holt_winters_with_fit must be greater than 0, got 0`},
		{s: `SELECT holt_winters_with_fit(min(value), false, 2) FROM myseries where time < now() and time > now() - 1d GROUP BY time(1d)`, err: `expected integer argument as second arg in holt_winters_with_fit`},
		{s: `SELECT holt_winters_with_fit(min(value), 10, 'string') FROM myseries where time < now() and time > now() - 1d GROUP BY time(1d)`, err: `expected integer argument as third arg in holt_winters_with_fit`},
		{s: `SELECT holt_winters_with_fit(min(value), 10, -1) FROM myseries where time < now() and time > now() - 1d GROUP BY time(1d)`, err: `third arg to holt_winters_with_fit cannot be negative, got -1`},
		{s: `SELECT mean(value) + value FROM air WHERE time < now() and time > now() - 1h GROUP BY time(10m)`, err: `mixing aggregate and non-aggregate queries is not supported`},
		{s: `SELECT mean(air_total - air_idle) FROM air`, err: `expected field argument in mean()`},
		{s: `SELECT derivative(mean(air_total - air_idle), 1s) FROM air WHERE time < now() AND time > now() - 1d GROUP BY time(1h)`, err: `expected field argument in mean()`},
		{s: `SELECT count(foo + sum(bar)) FROM air`, err: `expected field argument in count()`},
		{s: `SELECT (count(foo + sum(bar))) FROM air`, err: `expected field argument in count()`},
		{s: `SELECT sum(value) + count(foo + sum(bar)) FROM air`, err: `expected field argument in count()`},
		{s: `SELECT top(value, 2), max(value) FROM air`, err: `selector function top() cannot be combined with other functions`},
		{s: `SELECT bottom(value, 2), max(value) FROM air`, err: `selector function bottom() cannot be combined with other functions`},
		{s: `SELECT min(derivative) FROM (SELECT derivative(mean(value), 1h) FROM myseries) where time < now() and time > now() - 1d`, err: `derivative aggregate requires a GROUP BY interval`},
		{s: `SELECT min(mean) FROM (SELECT mean(value) FROM myseries GROUP BY time)`, err: `time() is a function and expects at least one argument`},
		{s: `SELECT value FROM myseries WHERE value OR time >= now() - 1m`, err: `invalid condition expression: value`},
		{s: `SELECT value FROM myseries WHERE time >= now() - 1m OR value`, err: `invalid condition expression: value`},
		{s: `SELECT value FROM (SELECT value FROM air ORDER BY time DESC) ORDER BY time ASC`, err: `subqueries must be ordered in the same direction as the query itself`},
		{s: `SELECT sin(value, 3) FROM air`, err: `invalid number of arguments for sin, expected 1, got 2`},
		{s: `SELECT cos(2.3, value, 3) FROM air`, err: `invalid number of arguments for cos, expected 1, got 3`},
		{s: `SELECT tan(value, 3) FROM air`, err: `invalid number of arguments for tan, expected 1, got 2`},
		{s: `SELECT asin(value, 3) FROM air`, err: `invalid number of arguments for asin, expected 1, got 2`},
		{s: `SELECT acos(value, 3.2) FROM air`, err: `invalid number of arguments for acos, expected 1, got 2`},
		{s: `SELECT atan() FROM air`, err: `invalid number of arguments for atan, expected 1, got 0`},
		{s: `SELECT sqrt(42, 3, 4) FROM air`, err: `invalid number of arguments for sqrt, expected 1, got 3`},
		{s: `SELECT abs(value, 3) FROM air`, err: `invalid number of arguments for abs, expected 1, got 2`},
		{s: `SELECT ln(value, 3) FROM air`, err: `invalid number of arguments for ln, expected 1, got 2`},
		{s: `SELECT log2(value, 3) FROM air`, err: `invalid number of arguments for log2, expected 1, got 2`},
		{s: `SELECT log10(value, 3) FROM air`, err: `invalid number of arguments for log10, expected 1, got 2`},
		{s: `SELECT pow(value, 3, 3) FROM air`, err: `invalid number of arguments for pow, expected 2, got 3`},
		{s: `SELECT atan2(value, 3, 3) FROM air`, err: `invalid number of arguments for atan2, expected 2, got 3`},
		{s: `SELECT sin(1.3) FROM air`, err: `field must contain at least one variable`},
		{s: `SELECT nofunc(1.3) FROM air`, err: `undefined function nofunc()`},
	} {
		t.Run(tt.s, func(t *testing.T) {
			stmt, err := cnosql.ParseStatement(tt.s)
			if err != nil {
				t.Fatalf("unexpected error: %s", err)
			}
			s := stmt.(*cnosql.SelectStatement)

			opt := query.CompileOptions{}
			if _, err := query.Compile(s, opt); err == nil {
				t.Error("expected error")
			} else if have, want := err.Error(), tt.err; have != want {
				t.Errorf("unexpected error: %s != %s", have, want)
			}
		})
	}
}

func TestPrepare_MapShardsTimeRange(t *testing.T) {
	for _, tt := range []struct {
		s          string
		start, end string
	}{
		{
			s:     `SELECT max(value) FROM air WHERE time >= '2022-09-03T15:00:00Z' AND time <= '2022-09-03T16:00:00Z' GROUP BY time(10m)`,
			start: "2022-09-03T15:00:00Z",
			end:   "2022-09-03T16:00:00Z",
		},
		{
			s:     `SELECT derivative(mean(value)) FROM air WHERE time >= '2022-09-03T15:00:00Z' AND time <= '2022-09-03T16:00:00Z' GROUP BY time(10m)`,
			start: "2022-09-03T14:50:00Z",
			end:   "2022-09-03T16:00:00Z",
		},
		{
			s:     `SELECT moving_average(mean(value), 3) FROM air WHERE time >= '2022-09-03T15:00:00Z' AND time <= '2022-09-03T16:00:00Z' GROUP BY time(10m)`,
			start: "2022-09-03T14:30:00Z",
			end:   "2022-09-03T16:00:00Z",
		},
		{
			s:     `SELECT moving_average(mean(value), 3) FROM air WHERE time <= '2022-09-03T16:00:00Z' GROUP BY time(10m)`,
			start: "1677-09-21T00:12:43.145224194Z",
			end:   "2022-09-03T16:00:00Z",
		},
	} {
		t.Run(tt.s, func(t *testing.T) {
			stmt, err := cnosql.ParseStatement(tt.s)
			if err != nil {
				t.Fatalf("unexpected error: %s", err)
			}
			s := stmt.(*cnosql.SelectStatement)

			opt := query.CompileOptions{}
			c, err := query.Compile(s, opt)
			if err != nil {
				t.Fatalf("unexpected error: %s", err)
			}

			shardMapper := ShardMapper{
				MapShardsFn: func(_ cnosql.Sources, tr cnosql.TimeRange) query.ShardGroup {
					if got, want := tr.Min, mustParseTime(tt.start); !got.Equal(want) {
						t.Errorf("unexpected start time: got=%s want=%s", got, want)
					}
					if got, want := tr.Max, mustParseTime(tt.end); !got.Equal(want) {
						t.Errorf("unexpected end time: got=%s want=%s", got, want)
					}
					return &ShardGroup{}
				},
			}

			if _, err := c.Prepare(&shardMapper, query.SelectOptions{}); err != nil {
				t.Fatalf("unexpected error: %s", err)
			}
		})
	}
}

type ShardMapper struct {
	MapShardsFn func(sources cnosql.Sources, t cnosql.TimeRange) query.ShardGroup
}

func (m *ShardMapper) MapShards(sources cnosql.Sources, t cnosql.TimeRange, opt query.SelectOptions) (query.ShardGroup, error) {
	shards := m.MapShardsFn(sources, t)
	return shards, nil
}

type ShardGroup struct {
	CreateIteratorFn func(ctx context.Context, m *cnosql.Measurement, opt query.IteratorOptions) (query.Iterator, error)
	Fields           map[string]cnosql.DataType
	Dimensions       []string
}

func (sh *ShardGroup) CreateIterator(ctx context.Context, m *cnosql.Measurement, opt query.IteratorOptions) (query.Iterator, error) {
	return sh.CreateIteratorFn(ctx, m, opt)
}

func (sh *ShardGroup) IteratorCost(m *cnosql.Measurement, opt query.IteratorOptions) (query.IteratorCost, error) {
	return query.IteratorCost{}, nil
}

func (sh *ShardGroup) FieldDimensions(m *cnosql.Measurement) (fields map[string]cnosql.DataType, dimensions map[string]struct{}, err error) {
	fields = make(map[string]cnosql.DataType)
	dimensions = make(map[string]struct{})

	for f, typ := range sh.Fields {
		fields[f] = typ
	}
	for _, d := range sh.Dimensions {
		dimensions[d] = struct{}{}
	}
	return fields, dimensions, nil
}

func (sh *ShardGroup) MapType(m *cnosql.Measurement, field string) cnosql.DataType {
	if typ, ok := sh.Fields[field]; ok {
		return typ
	}
	for _, d := range sh.Dimensions {
		if d == field {
			return cnosql.Tag
		}
	}
	return cnosql.Unknown
}

func (*ShardGroup) Close() error {
	return nil
}

// mustParseTime parses an IS0-8601 string. Panic on error.
func mustParseTime(s string) time.Time {
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		panic(err.Error())
	}
	return t
}
