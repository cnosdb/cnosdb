<a href="https://codebeat.co/projects/github-com-cnosdatabase-cnosdb-main"><img alt="codebeat badge" src="https://codebeat.co/badges/23007af1-7b99-419c-81a8-7bfb6dac31b9" /></a>
![GitHub](https://img.shields.io/github/license/cnosdb/cnosdb)

# CnosDB

[English](./README.md) | 简体中文


一款高性能、高压缩率、高易用性的开源分布式时序数据库。

点击查看[路线图](./ROADMAP.md)

## 功能特性


- 超大数据规模
   - 分布式支持10亿以上的时间序列
   - 支持1000亿以上的数据点存储
   - 支持海量时间序列下的分布式聚合查询
- 快速的批量写入
   - 定义全新的内存和磁盘数据结构
   - 硬件资源的抽象和写入优化
   - 动态调整节点负载，优化数据偏斜下的性能表现
   - 多级存储策略，优化后端IO
- 超高数据压缩比
   - 列式存储
   - 多级压缩
   - 类型和分布自适应的压缩算法
   - 综合压缩比超过60倍
- 丰富的计算函数
   - 50多种计算函数
   - 接口模块化设计
- 优秀的生态系统
   - 原生支持k8s和docker
   - 支持Java，Go，C/C++，Python开发接口
   - 支持Telegraf, Grafana, Prometheus等第三方工具

## 加入社区
欢迎所有热爱时序数据库的开发者/用户参与到CnosDB User Group中。扫描下方二维码，加CC为好友，即可入群。

入群前请查看[入群须知](./CnosDBWeChatUserGroupGuidelines.md)

![](https://github.com/cnosdb/cnosdb/blob/main/doc/assets/u.jpg)

## 快速开始

> 如果需要完整的入门指南，请查看[入门指南](https://cnosdb.github.io/)


### 构建

1. 克隆项目

   ```
   git clone https://github.com/cnosdb/cnosdb.git
   ```

2. 编译

   ```
   go install ./...
   ```

### 运行

1. 启动

   ```bash
   $GOPATH/bin/cnosdb
   ```

2. 使用

   ```bash
   $GOPATH/bin/cnosdb-cli
   ```

## 使用指南

### 创建数据库

```
curl -i -XPOST http://localhost:8086/query --data-urlencode "q=CREATE DATABASE mydb"
```

### 写入数据

```
curl -i -XPOST 'http://localhost:8086/write?db=db' --data-binary 'cpu,host=server01,region=Beijing idle=0.72 1434055562000000000'
```

### 查询数据

```
curl -G 'http://localhost:8086/query?pretty=true' --data-urlencode "db=db" --data-urlencode "q=SELECT \"idle\" FROM \"cpu\" WHERE \"region\"='Beijing'"
```

## 如何贡献

请参照[贡献指南](./CONTRIBUTING.md)为CnosDB做贡献。

## 许可证

[MIT License](./LICENSE)

## 联系我们

* [Stack Overflow](https://stackoverflow.com/questions/tagged/cnosdb)

* Twitter: [@CnosDB](https://twitter.com/CnosDB)

* [领英主页](https://www.linkedin.com/company/cnosdb)

* [B站](https://space.bilibili.com/36231559)

* [抖音](https://www.douyin.com/user/MS4wLjABAAAA6ua1UPmYWCcTl0AT0Lf1asILf9ogmj7J257KEq812csox9FBrAkxxKcok1GIzPMv)

* [知乎](https://www.zhihu.com/org/cnosdb)

* [CSDN](https://blog.csdn.net/CnosDB)

* [简书](https://www.jianshu.com/u/745811688e9e)

* email: mkt@cnosdb.com
