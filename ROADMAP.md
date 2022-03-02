# RoadMap

## cnosdb1.0 RoadMap

- 0.10   2022-03-30
  目标：完善运维工具cnosdb_inspect，修复重要bug

- 0.11    2022-04-30
  目标：完善运维工具cnosdb_tools，修复重要bug

- 0.12    2022-05-30
  目标：完善运维工具cnosdb-ctl，修复重要bug

- 0.13    2022-06-30
  目标：完善go client sdk，java client sdk，添加prometheus 写入和查询服务，修复重要bug
  
## cnosdb-2.0 RoadMap

从更远的角度考虑，为了cnosdb拥有更好的安全、性能指标，计划使用Rust语言来开发cnosdb-2.0的版本，并实现查询引擎和存储引擎的插
件化，以获得更好的生态适配性。

在cnosdb-2.0重新设计存储引擎的过程中我们尽可能去解决当前时序数据库面临的一系列问题：比如时间线膨胀。以及如何与基于对象存储
的完全计算存储分离的TSDB形成一整套完整的时序解决方案，最终我们的形态应该为cnosdb-2.0 + 云原生

| title | content | time |
| :---: | :---: | :---: |
| 完成cnosdb-2.0存储引擎 | 完成基本的read/write/grpc | 2022-05-01 |
| 完成存储引擎和查询引擎适配（cnosdb-2.0） | 改造cnosdb当前的查询引擎为grpc call，复用cnosdb查询引擎 | 2022-07-15 |
| cnosdb-2.0 cluster | 完成cnosdb-next的集群版、查询引擎和存储引擎插件化 | 2022-09-30 |
| ecosystem | 生态系统开发，上下游软件生态代码社区贡献 |  |
| 云原生和多租户适配 | 多个云厂商的上架，适配 |  |

cnosdb-2.0 在设计上 采用模块化的设计方案 模块间采用rpc进行通信，这样query层能够复用当前的cnosdb-1.0的query层实现逐步迭代。