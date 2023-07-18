#!/usr/bin/env bash

# ./run_queries.sh <query_type>
# e.g.
#   TSBS_RUN_QUERIES_CMD=/Users/yukkit/Downloads/mac-arm64-cnosdb/run_queries_cnosdb \
#     ./benchmark/tsbs/run_queries.sh

set -e

current_dir=$(dirname "$0")

source ${current_dir}/shell_env.sh

TSBS_RUN_QUERIES_CMD=${TSBS_RUN_QUERIES_CMD:-"run_queries_cnosdb"}

QUERY_HOST=${QUERY_HOST:-"localhost"}
QUERY_POET=${QUERY_POET:-"8902"}
DB_NAME=${DB_NAME:-"benchmark"}

TSBS_WORKERS=${TSBS_SEED:-8}

TSBS_QUERY_DIR=${TSBS_QUERY_DIR:-"/data/queries"}

QUERY_TYPE=$1

function append_result() {
    local query_num=$1
    local seq=$2
    local value=$3
    if [[ $seq -eq 1 ]]; then
        jq ".result += [[${value}]]" <${RESULT_PATH} >"${RESULT_PATH}.tmp" && mv "${RESULT_PATH}.tmp" ${RESULT_PATH}
    else
        jq ".result[${query_num}] += [${value}]" <${RESULT_PATH} >"${RESULT_PATH}.tmp" && mv "${RESULT_PATH}.tmp" ${RESULT_PATH}
    fi
}

function run_query() {
    query=$1
    result=$(${TSBS_RUN_QUERIES_CMD} --db-name=${DB_NAME} --workers=${TSBS_WORKERS} --urls http://${QUERY_HOST}:${QUERY_POET} --file ${query} |
        grep "min:" |
        # min:   280.27ms, med:  1366.21ms, mean:  1705.85ms, max: 3920.89ms, stddev:   987.03ms, sum:  85.3sec, count: 50
        sed -n 2p |
        # 1352.62ms,
        awk '{print $6}' |
        # 1352.62
        sed 's/ms//g' |
        # 1.35262
        awk '{print $1/1000}')

    if [ -z $result ]; then
        echo "null"
    else
        echo $result
    fi
}

TRIES=6
for i in "${!QUERY_TYPES[@]}"; do
    query_num=${i}
    query_type=${QUERY_TYPES[$i]}

    echo "Running Q${query_num}: ${query_type}"

    if [[ $(uname) == 'Linux' ]]; then
        sync
        echo 3 | sudo tee /proc/sys/vm/drop_caches
    fi

    for j in $(seq 1 $TRIES); do
        RES=$(run_query ${TSBS_QUERY_DIR}/${query_type}.txt)
        [[ $RES != "" ]] &&
            append_result "$query_num" "$j" "$RES" ||
            append_result "$query_num" "$j" "null"
    done
done
