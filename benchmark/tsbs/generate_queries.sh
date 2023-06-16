#!/usr/bin/env bash

# ./generate_queries.sh <query_type>
# The generated file is in the ${TSBS_QUERY_DIR:-"/tmp"} directory and is named ${QUERY_TYPE}.gz
# e.g.
#   TSBS_GENERATE_QUERIES_CMD=/Users/yukkit/Downloads/mac-arm64-cnosdb/generate_queries ./benchmark/tsbs/generate_queries.sh low-fuel

#set -e

#current_dir=$(dirname "$0")

#source ${current_dir}/shell_env.sh

#TSBS_GENERATE_QUERIES_CMD=${TSBS_GENERATE_QUERIES_CMD:-"generate_queries"}

#TSBS_QUERY_NUM=${TSBS_QUERY_NUM:-50}
#TSBS_TIMESTAMP_START=${TSBS_TIMESTAMP_START:-"2022-01-01T00:00:00Z"}
#TSBS_TIMESTAMP_END=${TSBS_TIMESTAMP_END:-"2022-02-01T00:00:00Z"}

#TSBS_SEED=${TSBS_SEED:-123}
#TSBS_SCALE=${TSBS_SCALE:-4000}

#TSBS_QUERY_DIR=${TSBS_QUERY_DIR:-"/tmp"}

#for type in ${QUERY_TYPES[@]}; do
#    echo "Generating query of ${type}..."
#    ${TSBS_GENERATE_QUERIES_CMD} --use-case="iot" --seed=123 --scale=4000 \
#        --timestamp-start=${TSBS_TIMESTAMP_START} \
#        --timestamp-end=${TSBS_TIMESTAMP_END} \
#        --queries=${TSBS_QUERY_NUM} --query-type=${type} --format="cnosdb" |
#        gzip >${TSBS_QUERY_DIR}/${type}.gz
#    echo "Generated query of ${type} in ${TSBS_QUERY_DIR}/${type}.gz"
#done
