cleanup database file
```bigquery
$cd ~  && rm -rf .cnosdb
```

```
start service $cnosdb
```

```sh
URL=http://127.0.0.1:8086 go test ./tests
```