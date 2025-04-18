#!/bin/bash

function usage() {
  echo 'Start CnosDB Server and run tests'
  echo
  echo 'USAGE:'
  echo "    ${0} [OPTIONS]"
  echo
  echo 'OPTIONS:'
  echo '    --singleton         Run singleton CnosDB Server (with meta-service included)'
  echo '    --1meta1data        Run CnosDB Server Cluster (1 meta, 1 data:query_tskv)'
  echo '    --1meta3data        Run CnosDB Server Cluster (1 meta, 3 data:query_tskv)'
  echo '    --3meta3data        Run CnosDB Server Cluster (3 meta, 3 data:query_tskv)'
  echo '    --1meta1query1tskv  Run CnosDB Server Cluster (1 meta, 1 data:query, 1 data:tskv)'
  echo '    -h | --help         Show this help message'
  echo
}

set -e

# This script is at $PROJ_DIR/query_server/sqllogicaltests/script/start_and_test.sh
PROJ_DIR=$(
  cd $(dirname $0)
  cd ../../..
  pwd
)

META="${PROJ_DIR}/meta/scripts/cluster.sh"
DATA="${PROJ_DIR}/main/scripts/cluster.sh"
PROFILE='release'

## Run cluster.
CLUSTER_STARTED=0
while [[ $# -gt 0 ]]; do
  key=${1}
  case ${key} in
  --singleton)
    $DATA --profile $PROFILE --system-database-replica 1
    CLUSTER_STARTED=1
    ;;
  --1meta1data)
    $META --node-num 1 --system-database-replica 1
    $DATA --profile $PROFILE --query-tskv-num 1 --system-database-replica 1
    CLUSTER_STARTED=1
    ;;
  --1meta3data)
    $META --node-num 1 --system-database-replica 3
    $DATA --profile $PROFILE --query-tskv-num 3 --system-database-replica 3
    CLUSTER_STARTED=1
    ;;
  --3meta3data)
    $META --node-num 3 --system-database-replica 3
    $DATA --profile $PROFILE --query-tskv-num 3 --system-database-replica 3
    CLUSTER_STARTED=1
    ;;
  --1meta1query1tskv)
    $META --node-num 1 --system-database-replica 1
    $DATA --profile $PROFILE --query-num 1 --tskv-num 1 --system-database-replica 1
    CLUSTER_STARTED=1
    ;;
  -h | --help)
    usage
    exit 0
    ;;
  *)
    usage
    exit 1
    ;;
  esac

  break
done

if [ $CLUSTER_STARTED -eq 0 ]; then
  echo "Cnosdb Cluster not started, maybe missing argument --singleton or other cluster definitions."
  exit 1
fi

## Test cluster.

TEST_DATA_DIR='/tmp/cnosdb/'
LOG_PATH='/tmp/cnosdb/data/1/nohup.log'
HTTP_HOST=${HTTP_HOST:-"127.0.0.1:8902"}
PING_URL="http://${HTTP_HOST}/api/v1/ping"

echo "=== CnosDB Start&Test ==="
echo "----------------------------------------"

trap "echo 'Received Ctrl+C, stopping.'" SIGINT

echo "Waiting for CnosDB Server startup to complete."
PID=$(cat /tmp/cnosdb/data/1/PID)
while [ -z "$(curl -s ${PING_URL})" ]; do
  if ! kill -0 ${PID} >/dev/null 2>&1; then
    echo "CnosDB Server startup failed, printing logs:"
    cat $LOG_PATH
    exit 1
  fi
  sleep 2
done

function test() {
  echo "= Testing sqllogicaltests"
  cargo run --package sqllogicaltests
  # echo "= Testing e2e_test"
  # cargo test --package e2e_test --lib -- reliant:: --nocapture
}

test || EXIT_CODE=$?

echo "= Test completed, killing CnosDB Server(pid=${PID}) and cleaning up '${TEST_DATA_DIR}'."
kill ${PID}
# rm -rf ${TEST_DATA_DIR}

exit ${EXIT_CODE:-0}
