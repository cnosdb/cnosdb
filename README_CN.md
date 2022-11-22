<img alt="" src="docs/source/_static/img/cnosdb_logo_white.svg" width="360"/>

<a href="https://codebeat.co/projects/github-com-cnosdatabase-cnosdb-main"><img alt="codebeat badge"
src="https://codebeat.co/badges/23007af1-7b99-419c-81a8-7bfb6dac31b9"/></a>
![GitHub](https://img.shields.io/github/license/cnosdb/cnosdb)

[English](./README.md) | 简体中文

## CnosDB2.0 的设计目标

设计并开发一个高性能、高压缩比、高可用的分布式云原生时间序列数据库，满足以下目标：

> 时序数据库

1. 扩展性，理论上支持的时间序列无上限，彻底解决时间序列膨胀问题，支持横/纵向扩展。
2. 计算存储分离，计算节点和存储节点，可以独立扩缩容，秒级伸缩。
3. 高性能存储和低成本，利用高性能io栈，支持利用云盘和对象存储进行分级存储。
4. 查询引擎支持矢量化查询。
5. 支持多种时序协议写入和查询，提供外部组件导入数据。

> 云原生

1. 支持云原生，支持充分利用云基础设施带来的便捷，融入云原生生态。
2. 高可用性，秒级故障恢复，支持多云，跨区容灾备灾。
3. 原生支持多租户，按量付费。
4. CDC,日志可以提供订阅和分发到其他节点。
5. 为用户提供更多可配置项，来满足公有云用户的多场景复杂需求。
6. 云边端协同，提供边端与公有云融合的能力
7. 融合云上OLAP/CloudAI 数据生态系统。

## CnosDB 整体架构

![整体架构](./docs/source/_static/img/arch.jpg)

## 路线图

* [路线图](./docs/roadmap/ROADMAP_CN.md)

## 加入社区

欢迎所有热爱时序数据库的开发者/用户参与到CnosDB User Group中。扫描下方二维码，加CC为好友，即可入群。

入群前请查看[入群须知](./docs/guidelines/CnosDBWeChatUserGroupGuidelines.md)

<img src="docs/source/_static/img/u.jpg" width="300" alt=""/>

## 使用CnosDB

点击[使用CnosDB](docs/quick-start-cn.md)快速开始

## 社区贡献指南

请参照[贡献指南](CONTRIBUTING.md)成为CnosDB的Contributor。

## 联系我们

* [官方主页](https://www.cnosdb.com)

* [Stack Overflow](https://stackoverflow.com/questions/tagged/cnosdb)

* [推特:@CnosDB](https://twitter.com/CnosDB)

* [领英主页](https://www.linkedin.com/company/cnosdb)

* [B站](https://space.bilibili.com/36231559)

* [抖音](https://www.douyin.com/user/MS4wLjABAAAA6ua1UPmYWCcTl0AT0Lf1asILf9ogmj7J257KEq812csox9FBrAkxxKcok1GIzPMv)

* [知乎](https://www.zhihu.com/org/cnosdb)

* [CSDN](https://blog.csdn.net/CnosDB)

* [简书](https://www.jianshu.com/u/745811688e9e)

## 我们正在招聘

* 如果您对全职、兼职或者实习工作感兴趣，请发简历到 hr@cnosdb.com

## 许可证

* [AGPL-3.0 License](./LICENSE.md)
