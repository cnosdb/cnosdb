#!/usr/bin/env bash

# ./execute_queries.sh

set -e

current_dir=$(dirname "$0")

function append_result() {
    local query_num=$1
    local seq=$2
    local value=$3
    if [[ $seq -eq 1 ]]; then
        jq ".result += [[${value}]]" <${RESULT_PATH} >${RESULT_PATH}.tmp && mv ${RESULT_PATH}.tmp ${RESULT_PATH}
    else
        jq ".result[${query_num}] += [${value}]" <${RESULT_PATH} >${RESULT_PATH}.tmp && mv ${RESULT_PATH}.tmp ${RESULT_PATH}
    fi
}

function run_query() {
    local query_num=$1
    local seq=$2
    local query=$3

    echo "$query" >/home/gitlab-runner/query.sql

    RES=$(${SQL_CLI} -f /home/gitlab-runner/query.sql 2>&1 | grep "Query took" | sed -n 1p | awk '{print $3}')
    [[ $RES != "" ]] &&
        append_result "$query_num" "$seq" "$RES" ||
        append_result "$query_num" "$seq" "null"
}

TRIES=6
QUERY_NUM=0
while read -r query; do
    echo "Running Q${QUERY_NUM}: ${query}"

    if [[ $(uname) == 'Linux' ]]; then
        sync
        echo 3 | sudo tee /proc/sys/vm/drop_caches
    fi

    for i in $(seq 1 $TRIES); do
        run_query "$QUERY_NUM" "$i" "$query"
    done
    QUERY_NUM=$((QUERY_NUM + 1))
done <"${current_dir}/sql/queries.sql"
