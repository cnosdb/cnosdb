# RoadMap

## cnosdb-v1 RoadMap

| title  |  content   |  time   |
|:------:|:----------:|:-------:|
| 集群一致性  | 集群一致性和功能增强 | 2020-07 |
|  权限管理  | 权限管理和备份还原  | 2020-08 |
| 社区生态支持 | 社区生态以及查询增强 | 2022-09 |
|  存储适配  | 存储适配，分层存储  | 2022-10 |


### 集群一致性和功能增强
- 增加命令行工具选项
- 增加UDP接口功能
- 支持CnosQL的IN语法
- 支持将Mysql和PostgreSQL导入到CnosDB
- 集群最终一致性


### 权限管理和备份还原
- 支持用户对数据库的读写权限
- 支持measurement的权限控制
- 支持cnosdb-meta节点权限控制
- 支持https
- 集群的备份和还原

### 社区生态支持以及查询
- Python SDK
- C# SDK
- 支持DBverser查询
- 支持Grafana查询
- 支持显示服务器状况的功能
- 支持按自然年和月进行分组


### 存储适配
- 分层存储
- 适配Azure
- 适配S3


cnosdb-v1 release

> benchtest 参考[tsdb-comparisons](https://github.com/cnosdb/tsdb-comparisons)

> 即将release文档 [repo](https://github.com/cnosdb/docs/tree/latest)