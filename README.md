<a href="https://codebeat.co/projects/github-com-cnosdatabase-cnosdb-main"><img alt="codebeat badge" src="https://codebeat.co/badges/23007af1-7b99-419c-81a8-7bfb6dac31b9" /></a>
![GitHub](https://img.shields.io/github/license/cnosdatabase/cnosdb)

# CnosDB

English | [简体中文](./README.cn.md)

An Open Source Distributed Time Series Database with high performance, high compression ratio and high usability.

## Features

- High batch writing throughput.

- High compression ratio.

- Rich calculation algorithms.

## Join the community
All developers/users who love time series databases are welcome to participate in the CnosDB User Group. Scan the QR code below and add CC to join the group.

Please check [Instructions for joining the group](./CnosDBWeChatUserGroupGuidelines.md) beforehand.

![](https://github.com/cnosdatabase/cnosdb/blob/main/doc/assets/u.jpg)

## Quick start

> If you need a complete getting started guide, please check the [Quickstart Guide](https://cnosdatabase.github.io/)

### Construct

1. Clone

   ```
   git clone https://github.com/cnosdatabase/cnosdb.git
   ```

2. Compile

   ```
   go install ./...
   ```

### Operation

1. Start

   ```bash
   $GOPATH/bin/cnosdb
   ```

2. Use

   ```bash
   $GOPATH/bin/cnosdb-cli
   ```

## User's Guide

### Create database

```
curl -i -XPOST http://localhost:8086/query --data-urlencode "q=CREATE DATABASE mydb"
```

### Insert data

```
curl -i -XPOST 'http://localhost:8086/write?db=db' --data-binary 'cpu,host=server01,region=Beijing idle=0.72 1434055562000000000'
```

### Query

```
curl -G 'http://localhost:8086/query?pretty=true' --data-urlencode "db=db" --data-urlencode "q=SELECT \"idle\" FROM \"cpu\" WHERE \"region\"='Beijing'"
```

## How to contribute

Please refer to [Contribution Guide](./CONTRIBUTING.md) to contribute to CnosDB.

## License

[MIT License](./LICENSE)
