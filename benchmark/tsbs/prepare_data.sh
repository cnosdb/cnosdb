#!/usr/bin/env bash

# 下载数据，数据入库
# ./prepare_data.sh
# e.g.
#   TSBS_DATA_URL=/tmp/cnosdb-data.gz \
#   TSBS_LOAD_CMD=/Users/yukkit/Downloads/mac-arm64-cnosdb/load_cnosdb \
#   TSBS_LOAD_URL=http://127.0.0.1:8902 \
#   TSBS_LOAD_WORKERS=24 \
#     ./benchmark/tsbs/prepare_data.sh

set -e

current_dir=$(dirname "$0")

#if [[ ${TSBS_DATA_URL} =~ "^http.*" ]]; then
#    wget --continue ${TSBS_DATA_URL} -O ${TMP_DIR}/data.gz
#else
#    cp ${TSBS_DATA_URL} ${TMP_DIR}/data.gz
#fi

SQL_CLI=${SQL_CLI:-"cnosdb-cli"}

${SQL_CLI} --file ${current_dir}/sql/create.sql >/dev/null
# cat /data/cnosdb_iot_123_2022.gz| gunzip | load_cnosdb --urls http://127.0.0.1:8902 --workers 50
RES_METRICS_AND_TIME=$(cat ${TSBS_DATA_URL} | gunzip | ${TSBS_LOAD_CMD} --urls ${TSBS_LOAD_URL} --workers ${TSBS_LOAD_WORKERS} 2>&1 | grep "rows in" | awk '{print $2, $5}')
# loaded_metrics=$(echo ${RES_METRICS_AND_TIME} | awk '{print $1}' | bc -l)
# load_time=$(echo ${RES_METRICS_AND_TIME} | awk '{print $2}' | bc -l)
# echo "Data loaded ${load_metrics} metrics in ${load_time}."
echo ${RES_METRICS_AND_TIME//sec/}
