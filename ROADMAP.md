# RoadMap

## cnosdb-1.0 RoadMap
> 点击查看 [cnosdb-2.0 RoadMap](#cnosdb-20-roadmap)

|       title        |            content            |    time    |
|:------------------:|:-----------------------------:|:----------:|
|        SDK         |      开发Java和Golang的客户端程序      | 2022-03-15  |
| Prometheus adapter |      开发与Prometheus的集成程序        | 2022-03-21  |
|     cnosdb         |          backup/restore             | 2022-03-31  |
|      JDBC          |         提供JDBC客户端程序            |              |
|   cnosdb_inspect   | 完成cnosdb_inspect中关于磁盘上文件的所有操作 | 2022-04 |
|    cnosdb_tools    |     完成cnosdb_tools中手动运维工具     | 2022-04   |
|     cnosdb-ctl     |            集群运维工具             |             |
|         解耦        |       解耦查询和存储，使用GRPC通信        | 2022-07-15 |

cnosdb_inspect包含以下内容：
- buildtsi：重建tsi索引文件
- deletetsm：按条件删除.tsm文件中的数据
- dumptsi：转储tsi文件中的内容
- dumptsm：转储tsm文件中的内容
- dumptsmwal：转储wal文件中的内容
- export：将CnosDB中的数据导出为 Line Protocol 格式的数据，可以使用cnosdb-cli import导入到CnosDB
- report：显示所有分片中series的元数据
- reporttsi：计算series基数的精确计数
- verify：验证verify文件的完整性
- Verify-seriesfile：验证series文件的完整性
- reportdisk：查看TSM文件磁盘和分片的使用情况，用来评估磁盘规划
- verify-tombstone：验证tombstone文件的完整性

cnosdb_tools包含以下内容：
- compact-shard：手动压缩分片
- export：与 cnosdb-tools import 一起使用，可以将分片导出为新的持续时间
- import：与 cnosdb-tools export 一起使用，可以将分片导入为新的保留策略
- gen-init：生成存储描述
- gen-exec：为每个分片和series生成数据

cnosdb-ctl包含以下内容：
- backup：备份数据
- restore：还原备份的数据
- copy-shard：将分片从一个节点复制到另一个节点
- copy-shard-status：查看copy-shard任务状态
- kill-copy-shard：强制结束copy-shard任务
- truncate-shards：截断集群中所有热分片，并从当前时间点新建一批分片
- update-data：更新meta服务中存储的data节点的主机名称

SDK：
- Golang
- Java
- C#
- Python
- ...

JDBC:提供JDBC客户端程序


与Pormetheus集成
- 添加Prometheus adapter，用于使用prometheus读写CnosDB

为了cnosdb能够稳定地提供数据的维护工作，需要完成以上任务。
从长远的角度来看，为了能够与cnosdb-2.0相结合，需要解耦cnosdb1.0的查询层和存储层，中间使用GRPC通信






## cnosdb-2.0 RoadMap
> 点击查看 [cnosdb-1.0 RoadMap](#cnosdb-10-roadmap)

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
