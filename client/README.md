# CnosDB Command-line Interface

The CnosDB CLI allows SQL queries to be executed by communicating with a remote CnosDB server.

Fork DataFusion.

```ignore
Command Line Client for Cnosdb.

USAGE:
    client [OPTIONS]

OPTIONS:
    -d, --database <DATABASE>
            Default database to connect to the CnosDB [default: public]

        --data-path <DATA_PATH>
            Path to your data, default to current directory

    -f, --file <FILE>...
            Execute commands from file(s), then exit

        --format <FORMAT>
            [default: table] [possible values: csv, tsv, table, json, nd-json]

    -h, --host <HOST>
            CnosDB server http api host [default: localhost]

        --help
            Print help information

    -p, --password <PASSWORD>
            Password used to connect to the CnosDB

    -P, --port <PORT>
            CnosDB server http api port [default: 31007]

    -q, --quiet
            Reduce printing other than the results and work quietly

        --rc <RC>...
            Run the provided files on startup instead of ~/.cnosdbrc

    -t, --target-partitions <TARGET_PARTITIONS>
            Number of partitions for query execution. Increasing partitions can increase
            concurrency.

    -u, --user <USER>
            The user name used to connect to the CnosDB [default: cnosdb]

    -V, --version
            Print version information
```

## Example

Create a CSV file to query.

```bash,ignore
$ echo "1,2" > data.csv
```

```sql,ignore
$ cnosdb-cli

CnosDB CLI v8.0.0

> CREATE EXTERNAL TABLE foo (a INT, b INT) STORED AS CSV LOCATION 'data.csv';
0 rows in set. Query took 0.001 seconds.

> SELECT * FROM foo;
+---+---+
| a | b |
+---+---+
| 1 | 2 |
+---+---+
1 row in set. Query took 0.017 seconds.
```

## CnosDB-Cli

Build the `client`.

```bash
cd cnosdb/client
cargo build
```
