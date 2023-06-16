#!/usr/bin/env bash

set -ex

# 机器型号
export MACHINE=${MACHINE:-$(uname -p)}
# 此次任务的名称
export BENCHMARK_ID=${BENCHMARK_ID:-$(git log -1 --pretty=%h)}
# 数据集
export BENCHMARK_DATASET=$1
# pr or release
export BENCHMARK_TYPE=${BENCHMARK_TYPE:-"pr"}

# hits
#export HITS_DATA_URL=${HITS_DATA_URL:-"/data/hits.parquet"}

# tsbs
export TSBS_LOAD_CMD=${TSBS_LOAD_CMD:-"load_cnosdb"}
export TSBS_LOAD_URL=${TSBS_LOAD_URL:-"http://127.0.0.1:8902"}
export TSBS_DATA_URL=${TSBS_DATA_URL:-"/data/cnosdb_iot_123_2022.gz"}
export TSBS_LOAD_WORKERS=${TSBS_LOAD_WORKERS:-24}
# tsbc optional
export TSBS_QUERY_NUM=${TSBS_QUERY_NUM:-24}
export TSBS_TIMESTAMP_START=${TSBS_TIMESTAMP_START:-"2022-01-01T00:00:00Z"}
export TSBS_TIMESTAMP_END=${TSBS_TIMESTAMP_END:-"2022-02-01T00:00:00Z"}
export TSBS_SEED=${TSBS_SEED:-123}
export TSBS_SCALE=${TSBS_SCALE:-4000}

# cnosdb cli
export CLI_CMD=${CLI_CMD:-"cnosdb-cli"}
export QUERY_HOST=${QUERY_HOST:-"127.0.0.1"}
export QUERY_PORT=${QUERY_PORT:-8902}

export SQL_CLI="${CLI_CMD} --host ${QUERY_HOST}  --port ${QUERY_PORT}"

# benchmark.sh 脚本的输出结果目录
export RESULT_DIR=${RESULT_DIR:-"./results"}
#export TMP_DIR="/tmp/cnosdb"
