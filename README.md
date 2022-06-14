<a href="https://codebeat.co/projects/github-com-cnosdatabase-cnosdb-main"><img alt="codebeat badge" src="https://codebeat.co/badges/23007af1-7b99-419c-81a8-7bfb6dac31b9" /></a>
![GitHub](https://img.shields.io/github/license/cnosdb/cnosdb)

# CnosDB

English | [简体中文](./README.cn.md)

An Open Source Distributed Time Series Database with high performance, high compression ratio and high usability.

Click to view [roadmap](./ROADMAP.md)

## Features

- Ultra-large data size
   - Distributed support for more than 1 billion time series
   - Support more than 100 billion data points storage
   - Support distributed aggregation query under massive time series
- Fast batch writing
   - Define new memory and disk data structure
   - Hardware resource abstraction and write optimization
   - Dynamic adjustment of node load to optimize performance under data skew
   - Multi-level storage strategy to optimize back-end IO
- Ultra-high data compression ratio
   - Column based storage
   - Multi-level compression
   - Type and distribution adaptive compression algorithms
   - Comprehensive compression ratio over 60 times
-Rich calculation functions
   - More than 50 calculation functions
   - Interface modular design
- Excellent ecosystem
   - Native support for k8s and docker
   - Support for Java, Go, C/C++, Python development interfaces
   - Support for third-party tools such as Telegraf, Grafana, Prometheus, etc.

## Join the community
All developers/users who love time series databases are welcome to participate in the CnosDB User Group. Scan the QR code below and add CC to join the group.

Please check [Instructions for joining the group](./CnosDBWeChatUserGroupGuidelines.md) beforehand.

![](https://github.com/cnosdb/cnosdb/blob/main/doc/assets/u.jpg)

## Quick start


> If you need a complete getting started guide, please check the [Quickstart Guide](https://cnosdb.github.io/)

### Construct

1. Clone

   ```
   git clone https://github.com/cnosdb/cnosdb.git
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

## Contact

* [Stack Overflow](https://stackoverflow.com/questions/tagged/cnosdb)

* Twitter: [@CnosDB](https://twitter.com/CnosDB)

* [LinkedIn page](https://www.linkedin.com/company/cnosdb)

* [Bilibili](https://space.bilibili.com/36231559)

* [Tiktok CN](https://www.douyin.com/user/MS4wLjABAAAA6ua1UPmYWCcTl0AT0Lf1asILf9ogmj7J257KEq812csox9FBrAkxxKcok1GIzPMv)

* [Zhihu](https://www.zhihu.com/org/cnosdb)

* [CSDN](https://blog.csdn.net/CnosDB)

* [Jianshu](https://www.jianshu.com/u/745811688e9e)

* email: hr@cnosdb.com
