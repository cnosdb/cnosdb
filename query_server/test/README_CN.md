# 文档

### 添加测试用例

在cases目录下添加 .sql .result 文件，注意.sql 文件和 .result 文件必须在同一目录下, \
且一一对应

sql文件名就是用例的名字，请确保用例名字唯一！

### 进行测试

请确认数据库已经启动!

开始测试
```shell
$ cargo run
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

### TODO

- [ ] 添加 test group filter 机制
- [ ] 支持行协议格式的sql文件
- [ ] 支持自启动DB