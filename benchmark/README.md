# CnosDB

## 前置条件

- 已安装 bc
- 已安装 jq
- 已安装 yq

## 生成 benchmark 结果

```bash
bash benchmark.sh
```

e.g.
```bash
BENCHMARK_ID=pr-1101 \
BENCHMARK_DATASET=tsbs \
BENCHMARK_TYPE=pr \
TSBS_LOAD_CMD=/Users/yukkit/Downloads/mac-arm64-cnosdb/load_cnosdb \
TSBS_DATA_URL=/tmp/cnosdb-data.gz \
TSBS_GENERATE_QUERIES_CMD=/Users/yukkit/Downloads/mac-arm64-cnosdb/generate_queries \
TSBS_RUN_QUERIES_CMD=/Users/yukkit/Downloads/mac-arm64-cnosdb/run_queries_cnosdb \
./benchmark.sh 
```

执行成功后，会在 ${RESULT_DIR} 目录下生成 benchmark 结果
```
├── results
│   └── tsbs
│       └── pr-1101.json
```

### 环境变量

#### 元数据

- MACHINE: 机器型号
- BENCHMARK_ID: 此次任务的名称
- BENCHMARK_DATASET: 数据集
- BENCHMARK_TYPE: pr or release

#### cnosdb cli
- CLI_CMD: cnosdb cli 命令，默认`cnosdb-cli`
- QUERY_HOST: 查询的目标数据库的host，默认`127.0.0.1`
- QUERY_PORT: 查询的目标数据库的port，默认`8902`

#### 功能性
- RESULT_DIR: benchmark.sh 脚本的输出结果目录，默认`./results`

#### hits 数据集

目前数据集名称为hits.parquet，默认路径为/data

#### tsbs 数据集

- TSBS_LOAD_CMD: load 命令，默认`load_cnosdb`
- TSBS_LOAD_URL: load 命令的目标数据库的地址， 默认`http://127.0.0.1:8902`
- TSBS_DATA_URL: tsbs 数据的路径， 默认`/data/cnosdb_iot_123_2022.gz`
- TSBS_LOAD_WORKERS: laod 任务的并发数， 默认`24`

#### tsbs 数据集
- TSBS_QUERY_NUM: 查询的数量，默认`24`
- TSBS_TIMESTAMP_START: 查询的开始时间，默认`2022-01-01T00:00:00Z`
- TSBS_TIMESTAMP_END: 查询的结束时间，默认`2022-02-01T00:00:00Z`
- TSBS_SEED: 默认`123`
- TSBS_SCALE: 默认`4000`

## 渲染 benchmark 结果

```bash
bash update-results.sh tsbs
```

将 ${RESULT_DIR}/tsbs 目录下的所有 json 文件渲染成 html 文件

### 参数

- $1: 数据集类型，可选`tsbs` or `hits`
- $2: ${RESULT_DIR} 的路径，默认`./results`
