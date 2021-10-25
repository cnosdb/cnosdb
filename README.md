# cnosdb

一个开源时序数据库。

## 功能特性

- 快速的批量写入
- 数据高压缩比
- 丰富的计算函数

## 快速开始

> 如果需要完整的入门指南，请查看[入门指南](https://cnosdatabase.github.io/)

### 构建

1. 克隆项目

   ```
   git clone https://github.com/cnosdatabase/cnosdb.git
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

<!-- 这里链接上该项目的开源许可证 -->

## 联系我们

<!-- 这里添加社区、博客和微信二维码 -->
