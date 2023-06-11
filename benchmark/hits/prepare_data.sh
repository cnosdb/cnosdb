#!/usr/bin/env bash

set -e

current_dir=$(dirname "$0")

q_start=$(date +%s.%N)
#if [[ ${HITS_DATA_URL} =~ "^http.*" ]]; then
    # Download benchmark target data
    # wget --continue https://datasets.clickhouse.com/hits_compatible/hits.parquet
#    wget --continue ${HITS_DATA_URL} -O /tmp/hits.parquet
#else
#    cp ${HITS_DATA_URL} /tmp/hits.parquet
#fi
${SQL_CLI} --file ${current_dir}/sql/create.sql >/dev/null
q_end=$(date +%s.%N)
q_time=$(echo "$q_end - $q_start" | bc -l)

data_size=$(wc -c </data/hits.parquet)

echo "${data_size} ${q_time}"
