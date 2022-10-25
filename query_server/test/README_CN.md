# 文档

### 添加测试用例

在cases目录下添加 .sql .result 文件，注意.sql 文件和 .result 文件必须在同一目录下, \
且一一对应

sql文件名就是用例的名字，请确保用例名字唯一！

添加测试时最好`CREATE DATABASE`，并且添加`--#DATABASE=db_name`的指令，这样有助于并行测试

### 进行测试

请确认数据库已经启动!

开始测试
```shell
cargo run --package test
```

### 并行测试

如果多个case可以并行测试，那么请在cases/TestGroup.toml中添加并行组

```toml
[[groups]]
name = "example" # 测试组的名字
tests = ["hello", "world"] # 测试组包含的多个用例
parallel = true # 是否并行
```
一个case可以被多个group包含

## 请求指令
提供多条指令控制查询请求

语法：

--#< instruction name >[= value]

#### 写line protocol
```
--#LP_BEGIN
m0,t0=a,t1=b f0=1.0 1
m1,t0=a,t1=b f0=1.0 1
...
--#LP_END
```

****
以下指令作用范围是，指令开始到文件结束

#### 设置请求的数据库
```
--#DATABASE=<databasename>
```
#### 设置用户名
```
--#USER_NAME=<username>
```
#### 设置是否排序
设置为true结果集会进行排序
```
--#SORT=<true|false>
```
#### 设置结果集是否美观返回
```
--#pretty=<true|false>
```
#### 设置是否超时失败
以秒为单位，超时则是ERROR
```
--#TIMEOUT=<seconds>
```



### TODO

- [ ] 添加 test group filter 机制
- [ ] 支持自启动DB
- [ ] 设定group中case执行顺序
- [ ] 语句级别并行
- 