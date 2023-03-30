# How to use sqllogicaltest for CnosDB

## run mode

- validation

The test framework will run all the use cases in the `slt` file and verify the results

```shell
cargo run --package sqllogicaltests
```

```shell
# run the `tpch.slt`
cargo run --package sqllogicaltests -- tpch
```

- completion

The test framework will run the test cases in the slt file and update the results to the slt file

```shell
cargo run --package sqllogicaltests --complete
```

```shell
# only update the `tpch.slt`
cargo run --package sqllogicaltests -- tpch --complete
```

## slt format

### valid result set

```sql
# <test_name>
query <type_string> <sort_mode>
<sql_query>
----
<expected_result>
```

- `test_name`: Uniquely identify the test name
- `type_string`: A short string that specifies the number of result columns and the expected datatype of each result column. There is one character in the <type_string> for each result column. The characters codes are:
  - "T" for a text result,
  - "I" for an integer result,
  - "R" for a floating-point result,
  - "?" for any other type.
- `expected_result`: The result set expected to be returned, the test framework will compare it with the actual result.

### check run results (success or failure)

error is a regex. Special characters change the meaning of a regex and have to be escaped

usage:
```sql
    statement [ count <rows> | ok | error <regex error message> ]
    <query statement>
```

e.g.
```slt
statement error The operation \(describe\) is not supported. Did you mean [describe]?
desc table example_basic;
```

### include file

An include copies all records from another files.

usage:
```sql
    include <file name>
```

### sleep

A sleep period.

usage:
```sql
    sleep <duration>
```

support duration format:

- `nsec`, `ns` -- nanoseconds
- `usec`, `us` -- microseconds
- `msec`, `ms` -- milliseconds
- `seconds`, `second`, `sec`, `s`
- `minutes`, `minute`, `min`, `m`
- `hours`, `hour`, `hr`, `h`
- `days`, `day`, `d`
- `weeks`, `week`, `w`
- `months`, `month`, `M` -- defined as 30.44 days
- `years`, `year`, `y` -- defined as 365.25 days
