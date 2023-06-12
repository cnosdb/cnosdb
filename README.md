<div align="center">
  <img  src="docs/source/_static/img/cnosdb_logo_white.svg" width="500" alt="CnodSB Logo">
</div>

<p align="center">
  <a href="https://github.com/cnosdb/cnosdb/actions">
  <img alt="CI" src="https://github.com/cnosdb/cnosdb/actions/workflows/makefile.yml/badge.svg" />
  </a>

  <a href="https://www.rust-lang.org/">
  <img alt="Rust" src="https://img.shields.io/badge/Language-Rust-blue.svg" />
  </a>

  <a href="https://github.com/cnoshb/cnosdb/blob/main/LICENSE.md">
  <img alt="License Agpl 3.0" src="https://img.shields.io/badge/License-AGPL_3.0-orange.svg" />
  </a>

  <a href="https://twitter.com/CnosDB">
  <img alt="twitter" src="https://img.shields.io/badge/twitter--white.svg?logo=twitter&style=social" />
  </a>

  <a href="https://www.linkedin.com/company/cnosdb">
  <img alt="linkedin" src="https://img.shields.io/badge/linkedin--white.svg?logo=linkedin&style=social" />
  </a>
</p>

<h3 align="center">
    <a href="https://www.cnosdb.com/">Website</a>
    •
    <a href="https://docs.cnosdb.com/">Documentation</a>
    •
    <a href="https://docs.cnosdb.com/en/quick_start.html">Quick Start</a>
</h3>

English | [简体中文](./README_CN.md)

CnosDB is a high-performance, high-compression, and easy-to-use open-source distributed time-series database. It is primarily used in fields such as IoT, industrial internet, connected cars, and IT operations. All of the code is open-sourced and available on GitHub.

In its design, we fully utilize the characteristics of time-series data, including structured data, non-transactions, fewer deletions and updates, more writes and less reads, etc. As a result, CnosDB has a number of advantages that set it apart from other time-series databases:

- **High performance**: CnosDB addresses the issue of time-series data expansion and theoretically supports unlimited time-series data. It supports aggregate queries along the timeline, including queries divided by equal intervals, queries divided by enumeration values of a column, and queries divided by the length of the time interval between adjacent time-series records. It also has caching capabilities for the latest data and the cache space can be configured for fast access to the latest data.
- **Easy to use**: CnosDB provides clear and simple interfaces, easy configuration options, standard SQL support, seamless integration with third-party tools, and convenient data access functions. It supports schema-less writing mode and supports historical data supplement(including out of order writing).
- **Cloud native**: CnosDB has a native distributed design, data sharding and partitioning, separation of storage and computing, Quorum mechanism, Kubernetes deployment and complete observability, ensuring final consistency. It can be deployed in public clouds, private clouds, and hybrid clouds. t also supports multi-tenancy and has role-based permission control. The computing and storage nodes support horizontal scaling.

# Architecture

![arch](./docs/source/_static/img/cnosdb_arch.png)

# Quick Start

## Build&Run from source

### **Support Platform**

We support the following platforms, if found to work on a platform not listed,
Please [report](https://github.com/cnosdb/cnosdb/issues) to us.

- Linux x86(`x86_64-unknown-linux-gnu`)
- Darwin arm(`aarch64-apple-darwin`)

### **Requirements**

1. Install `Rust`, You can check [official website](https://www.rust-lang.org/learn/get-started) to download and install
2. Install Cmake
```shell
# Debian or Ubuntu
apt-get install cmake
# Arch Linux
pacman -S cmake
# CentOS
yum install cmake
# Fedora
dnf install cmake
# macOS
brew install cmake
```

3. Install FlatBuffers

```shell
# Arch Linux
pacman -S flatbuffers
# Fedora
dnf install flatbuffers
# Ubuntu
snap install flatbuffers
# macOS
brew install flatbuffers
```

If your system is not listed, you can install FlatBuffers as follows

```shell
$ git clone -b v22.9.29 --depth 1 https://github.com/google/flatbuffers.git && cd flatbuffers

# Choose one of the following commands depending on your operating system
$ cmake -G "Unix Makefiles" -DCMAKE_BUILD_TYPE=Release
$ cmake -G "Visual Studio 10" -DCMAKE_BUILD_TYPE=Release
$ cmake -G "Xcode" -DCMAKE_BUILD_TYPE=Release

$ sudo make install
```

### **Compile**

```shell
git clone https://github.com/cnosdb/cnosdb.git && cd cnosdb
make build
```

### **Run**

#### Run CnosDB

The following is a single node startup. If you need to start a cluster, see [Start a CnosDB Cluster](https://docs.cnosdb.com/en/latest/deploy/install.html#start-a-cnosdb-cluster)

```bash
./target/debug/cnosdb run -M singleton --config ./config/config.toml
```

#### **Run CLI**
```shell
cargo run --package client --bin cnosdb-cli
```

## Run with Docker

1. Install [Docker](https://www.docker.com/products/docker-desktop/)

2. Start container
```shell
docker run --name cnosdb -d cnosdb/cnosdb:community-latest cnosdb run -M singleton --config /etc/cnosdb/cnosdb.conf
```
3. Run a command in the running container
```shell
docker exec -it cnosdb bash
```
4. Run `cnosdb-cli`
```shell
cnosdb-cli
```

> Quit `\q`
> Help `\?`
> For more details, check [quick start](https://docs.cnosdb.com/en/latest/start/quick_start.html)

## Write data

- [SQL](https://docs.cnosdb.com/en/latest/reference/sql.html#insert)
- [influxdb line-protocol](https://docs.influxdata.com/influxdb/v2.6/reference/syntax/line-protocol/)
- [bulk loading](https://docs.cnosdb.com/en/latest/develop/write.html#load-data)
- [telegraf](https://docs.cnosdb.com/en/latest/versatility/collect/telegraf.html)

The following will show an example of using cli to write data by SQL

1. CREATE TABLE

```sql
CREATE TABLE air (
    visibility DOUBLE,
    temperature DOUBLE,
    pressure DOUBLE,
    TAGS(station)
);
```

```bash
public ❯ CREATE TABLE air (
    visibility DOUBLE,
    temperature DOUBLE,
    pressure DOUBLE,
    TAGS(station)
);
Query took 0.063 seconds.
```

2. Insert a row

```sql
INSERT INTO air (TIME, station, visibility, temperature, pressure) VALUES
                (1673591597000000000, 'XiaoMaiDao', 56, 69, 77);
```

```bash
public ❯ INSERT INTO air (TIME, station, visibility, temperature, pressure) VALUES
                (1673591597000000000, 'XiaoMaiDao', 56, 69, 77);
+------+
| rows |
+------+
| 1    |
+------+
Query took 0.032 seconds.
```

3. insert multiple rows

```sql
INSERT INTO air (TIME, station, visibility, temperature, pressure) VALUES
                ('2023-01-11 06:40:00', 'XiaoMaiDao', 55, 68, 76),
                ('2023-01-11 07:40:00', 'DaMaiDao', 65, 68, 76);
```

```bash
public ❯ INSERT INTO air (TIME, station, visibility, temperature, pressure) VALUES
                ('2023-01-11 06:40:00', 'XiaoMaiDao', 55, 68, 76),
                ('2023-01-11 07:40:00', 'DaMaiDao', 65, 68, 76);
+------+
| rows |
+------+
| 2    |
+------+
Query took 0.038 seconds.
```

## Query data

- [SQL](https://docs.cnosdb.com/en/latest/reference/sql.html), compatible with SQL standard.
- [Prometheus remote read](https://docs.cnosdb.com/en/latest/versatility/collect/prometheus.html#remote-read).

The following will show an example of SQL query using cli

```sql
-- query table data
SELECT * FROM air;
```

```bash
public ❯ -- query table data
SELECT * FROM air;
+---------------------+------------+------------+-------------+----------+
| time                | station    | visibility | temperature | pressure |
+---------------------+------------+------------+-------------+----------+
| 2023-01-11T06:40:00 | XiaoMaiDao | 55         | 68          | 76       |
| 2023-01-13T06:33:17 | XiaoMaiDao | 56         | 69          | 77       |
| 2023-01-11T07:40:00 | DaMaiDao   | 65         | 68          | 76       |
+---------------------+------------+------------+-------------+----------+
Query took 0.036 seconds.
```

# Connector

CnosDB supports connections from various clients:

- C/C++
- Go
- Java
- Rust
- Python
- JDBC
- ODBC
- Arrow Filght SQL

Please refer to the "Connector" section in the documentation for the above examples. You can access it [here](https://docs.cnosdb.com/en/latest/reference/connector/).

# Roadmap

- Click to view [Roadmap](./docs/roadmap/ROADMAP.md)

# Join the community

All developers/users who love time series databases are welcome to participate in the CnosDB User Group. Scan the QR
code below and add CC to join the group.

Please check [Instructions for joining the group](./docs/guidelines/CnosDBWeChatUserGroupGuidelines.md) beforehand.

<img src="docs/source/_static/img/u.jpg" width="300" alt=""/>

## Contributing

Please refer to [Contribution Guide](./CONTRIBUTING_EN.md) to contribute to CnosDB.

# Acknowledgement

- CnosDB 2.0 uses [Apache Arrow](https://github.com/apache/arrow) as the memory model.
- CnosDB 2.0's query engine is powered by [Apache Arrow DataFusion](https://github.com/apache/arrow-datafusion).
- CnosDB 2.0's bug detection is powered by [SQLancer](https://github.com/sqlancer/sqlancer).
- CnosDB 2.0's integration test framework is powered by [sqllogictest-rs](https://github.com/risinglightdb/sqllogictest-rs).
