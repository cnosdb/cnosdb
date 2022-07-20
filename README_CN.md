
<img src="docs/source/_static/img/cnosdb_logo_white.svg" width="360"/>

<a href="https://codebeat.co/projects/github-com-cnosdatabase-cnosdb-main"><img alt="codebeat badge" src="https://codebeat.co/badges/23007af1-7b99-419c-81a8-7bfb6dac31b9" /></a>
![GitHub](https://img.shields.io/github/license/cnosdb/cnosdb)

[English](./README.md) | 简体中文


CnosDB是一款高性能、高压缩率、高易用性的开源分布式时序数据库；CnosDB依托于Rust, [Apache Arrow](https://arrow.apache.org/) 与 [DataFusion](https://github.com/apache/arrow-datafusion) 进行构建。

## CnosDB Isipho的设计目标
设计并开发一个高性能、高压缩比、高可用的分布式云原生时间序列数据库，满足以下目标：
### 存储
- 存算分离，时间序列膨胀（理论无上限 ）支持横/纵向扩展；
- 性能和成本，高性能io，Run-to-Completion调度模型，支持使用对象存储进行分级存储；
- 有损压缩，在用户可选择的情况下实现降精度的有损压缩；
### 查询
- 使用Apache Arrow及Datafusion实现查询引擎；
- 支持查询引擎矢量化的执行，执行复杂的查询语句；
- 支持标准SQL，Flux，支持丰富的聚合查询及算子。
### 生态
- 面向多租设计，提供更多的配置参数，能够提供资源更加灵活的配置；
- cdc、WAL可以提供订阅和分发到其他节点，更加灵活的部署和支持；
- 生态型兼容K8s生态，减少共享内存；
- 与其他数据生态系统相结合，支持导入/导出parquet文件；
- 兼容国际与国内主要公有云生态。
## 路线图
* [路线图](docs/roadmap/ROADMAP_CN.md)
## 加入社区
欢迎所有热爱时序数据库的开发者/用户参与到CnosDB User Group中。扫描下方二维码，加CC为好友，即可入群。

入群前请查看[入群须知](./docs/guidelines/CnosDBWeChatUserGroupGuidelines.md)

<img src="docs/source/_static/img/u.jpg" width="256"/>


## 社区贡献指南

- 请参照[贡献指南](CONTRIBUTING.md)为CnosDB做贡献。

## 联系我们

* [官方主页 (维护者中)](https://www.cnosdb.com)

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