GET STARTED



本地localhost测试

```
go test -v ./tests/
```

远程服务器测试

需设置环境变量URL

URL=http://ip:port

```
go test -parallel 1 ./tests
```







目前已覆盖的测试

#### 针对CnosQL测试：

- 简单数据查询

- 模式查询

- 数据管理
- 数学运算符(不全)



（以下是个人认为需添加的一些测试）

##### CnosQL函数的测试


| COUNT()                             |     |
| ----------------------------------- | --- |
| DISTINCT()                          |     |
| INTEGRAL()                          |     |
| MEAN()                              |     |
| MEDIAN()                            |     |
| MODE()                              |     |
| SPREAD()                            |     |
| STDDEV()                            |     |
| SUM()                               |     |
| BOTTOM()                            |     |
| FIRST()                             |     |
| LAST()                              |     |
| MAX()                               |     |
| MIN()                               |     |
| PERCENTILE()                        |     |
| SAMPLE()                            |     |
| TOP()                               |     |
| ABS()                               |     |
| ACOS()                              |     |
| ASIN()                              |     |
| ATAN()                              |     |
| ATAN2()                             |     |
| CEIL()                              |     |
| COS()                               |     |
| CUMULATIVE_SUM()                    |     |
| DERIVATIVE()                        |     |
| DIFFERENCE()                        |     |
| ELAPSED()                           |     |
| EXP()                               |     |
| FLOOR()                             |     |
| HISTOGRAM()                         |     |
| LN()                                |     |
| LOG()                               |     |
| LOG2()                              |     |
| LOG10()                             |     |
| MOVING_AVERAGE()                    |     |
| NON_NEGATIVE_DERIVATIVE()           |     |
| NON_NEGATIVE_DIFFERENCE()           |     |
| POW()                               |     |
| ROUND()                             |     |
| SIN()                               |     |
| SQRT()                              |     |
| TAN()                               |     |
| HOLT_WINTERS()                      |     |
| CHANDE_MOMENTUM_OSCILLATOR()        |     |
| EXPONENTIAL_MOVING_AVERAGE()        |     |
| DOUBLE_EXPONENTIAL_MOVING_AVERAGE() |     |
| KAUFMANS_EFFICIENCY_RATIO()         |     |
| KAUFMANS_ADAPTIVE_MOVING_AVERAGE()  |     |
| TRIPLE_EXPONENTIAL_MOVING_AVERAGE() |     |
| TRIPLE_EXPONENTIAL_DERIVATIVE()     |     |
| RELATIVE_STRENGTH_INDEX()           |     |



##### 数学运算符

https://docs.influxdata.com/influxdb/v1.7/query_language/math_operators/

- Addition(+)
- Subtraction(-) 
- Multiplication(*)（已测试）
- Division(/)
- Modulo(%)
- Bitwise AND(&)
- Bitwise OR(|)
- Bitwise Exclusive-OR(^)




##### 高级CnosQL测试

TODO

这部分具体的我也不清楚，只不过tests部分里面现在都是一些单表+一到两个tag+一到两个field的测试，然后查询语句都比较简单。

感觉需要添加一些更复杂的用例和查询语句



比如

子查询；

多表连接查询；

......



##### 连续查询的相关测试

TODO



#### 命令行测试

##### cnosdb

- cnosdb run
- cnosdb backup
- cnosdb restore

##### cnosdb-inspect

- deletetsm
- dumptsm
- dumptsi
- buildtsi
- dumptsmwal
- report-disk
- report
- reporttsi
- verify
- verify-seriesfile
- verify-tombstone
- export

##### cnosdb-tools

- cnosdb-tools compact
- cnosdb-tools export
- cnosdb-tools import
- cnosdb-tools gen-exec
- cnosdb-tools gen-init



#### 并发写入的测试

TODO

目的：改变并发进行数据写入的客户端数量，获取CnosDB在多客户端同时并发写时候的系统性能指标。



#### 并行查询的测试

TODO





#### 不同表长度的写入测试

TODO





#### 不同规模结果集查询测试

TODO







#### 用户权限测试

create user

drop user

grant语句测试



TODO



#### 异常测试

对于一些会引发异常错误的测试，因为上面的都是对“运行正确”的测试

需不需要对“运行异常”进行测试？就是弄一些一定会引发异常错误的用例。

比如名字包含特殊字符\，$，=，,，""等等。

















