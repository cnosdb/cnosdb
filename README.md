# CnosDB Isipho Road Map
## CnosDB Isipho的设计目标
设计并开发一个高性能、高压缩比、高可用的分布式云原生时间序列数据库，在CnosDB V1 的基础上，改进并满足以下目标：
### 存储
1. 存算分离，时间序列膨胀（理论无上限 ）支持横/纵向扩展； 
2. 性能和成本，高性能io，Run-to-Completion调度模型，支持使用对象存储进行分级存储；
3. 有损压缩，在用户可选择的情况下实现降精度的有损压缩；
### 查询
4. 使用Apache Arrow及Datafusion实现查询引擎；
5. 支持查询引擎矢量化的执行，执行复杂的查询语句；
6. 支持标准SQL，Flux，支持丰富的聚合查询及算子。
### 生态
7. 面向多租设计，提供更多的配置参数，能够提供资源更加灵活的配置；
8. cdc、wal 可以提供订阅和分发到其他节点，更加灵活的部署和支持；
9. 生态型兼容K8s生态，减少共享内存；
10. 与其他数据生态系统相结合，支持导入/导出parquet文件；
11. 兼容国际与国内主要公有云生态。
## CnosDB Isipho的模块划分
### Store Engine
重要模块： 
WAL：写前日志，用于停机后恢复 Memcache。
Memcache： memtable和 immutmemtable 内存中缓存数据。
TSM： 时序数据的列式存储格式。
Summary (TSM的MetaData) ：tsm文件版本变更产生的元数据文件，用于恢复数据。
Versionset：tskv全局视图，类似于manager。
Tsfamily： series的列簇，一个LSM的基本单元。
tsm的压缩：支持多种field类型的压缩。
tsm的有损压缩：支持降低数据精度的数据压缩。
支持操作：
1. 写操作：grpc -> wal -> memcache 。
2. 读操作： 支持point查询和range查询，能从memtable 和 tsm中读数据。
3. 标记式delete，通过compact删除文件， memtable中的数据实时清除。
4. flush:  immutcache 刷到 L0 层 较小的tsm文件。
5. compact:  tsm文件合并。
6. other： 配置文件， 支持从环境变量和配置读取。
### Query Engine
1. impl catalog provider
2. schema存储
3. 将tsm file中的数据组装成arrow中的recordbatch。 
4. table scan，解析tsm文件中的数据。 
5. system table & infomation schema 用于数据统计。
6. 正排索引
7. 倒排索引
8. db管理 
9. 查询优化
10. 定制化索引 
### 基础Lib库
1. fs： 用户态cache 的 direct IO。
2. schedule：Run-to-Completion 的模型。
### 分布式
1. 基于Rust语言的原生分布式系统
    a. 分布式架构、整体框架设计、计算存储分离
    b. 数据分片规则，基于一致性哈希
    c. 扩容缩容数据迁移
    d. 副本同步
    e. 运维工具
    f. 数据备份与还原
2. 实现数据最终一致性
    a. Hinted-Handoff
    b. 读修复
    c. 墓碑机制
    d. anti-entropy反熵
### 生态
因为CnosDB 1.0已经支持并兼容了很多优秀生态，所以Isipho会在一定程度上复用1.0的生态，并且做更多的支持。
1. CnosDB Isipho必须支持
    a. 行协议
    b. http 接口
2. 需要支持的生态伙伴产品
    a. 第三方与CnosDB的数据异构
	Telegraf（1.0 支持）
	Influxdb（1.0 支持）
	Prometheus（1.0 支持）
	Timescales
	Kafka/Pulsar
	MySQL/PgSQL（1.0 支持）
3. 数据管理工具涉及引擎层工具
    a. 数据文件备份（可以指定DB、shard、时间段等）与还原
    b. 导出为行协议（可以按照多个不同维度：DB、Shard、时间段）与批量导入
    c. 磁盘文件分析工具（分析tsm、wal、index文件等，索引重建，文件合法性校验等）
    d. 按照多个维度删除数据：DB、table、shard、时间段
### 测试支持
1. 基础测试
2. 持续压力测试
3. 集群全功能测试框架
4. 混沌测试
## CnosDB Isipho的时间表
### 202207 JULY GO 
kv:  
1. 读操作过滤删除  并补充测试用例
2. 删除操作完成  并补充测试用例  
3. compact merge iterator          
4. 支持离散点数据生产delta文件 
5. 补充基本测试用例 搭建ci 自动跑门禁 补充压测
6. 配置文件， 支持从环境变量和配置读取
7. 优化 不必要的lock  （长期工作）
query: 
1. impl catalog provider 
2. schema存储 对接arrow shema 
3. 将tsm file中的数据组装成arrow中的recordbatch。 
### 202208
### 202209
### 202210
### 202211
### 202212
