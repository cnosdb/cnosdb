# CnosDB Road Map

## CnosDB 2.4 设计目标

CnosDB 的设计目标是开发一个高性能、高压缩比、高可用性、分布式云原生时序数据库，CnosDB 2.4 将实现以下目标。

### 时序专用函数
常用函数（first、last、max、min）、日期转换类、监控类（gauges计算）。
### 有损压缩
- Deadband Compression ：一种数据压缩算法，用于降低传感器数据更新的频率，减少数据传输和存储成本。
- Swinging Door Trending (SDT)  Algorithm ：一种实时数据流处理算法，可以用于处理动态数据集合，通过不断调整门的大小来维护数据集中元素的数量。
### 支持Schema change 
支持 Update、Delete 等操作。
### 添加有主复制组 
实现处理流数据的 exactly once 语义。

## CnosDB 架构

![整体架构](../source/_static/img/arch.jpg)

## CnosDB 时间表

| 产品特性    | 时间节点  |
|------------| ----  |
| 2.4        | 2023.10 |



