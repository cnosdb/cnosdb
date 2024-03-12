#!/usr/bin/env bash
set -e

# define environment
export HTTP_HOST=${HTTP_HOST:-"127.0.0.1:8902"}
export URL="http://${HTTP_HOST}/api/v1/ping"

ARG_MODE='query_tskv'
ARG_CLEAN=0

function usage() {
  echo 'Start CnosDB Server and run tests'
  echo 'You need to start CnosDB Meta Server first, otherwise enable --singleton option.'
  echo
  echo 'USAGE:'
  echo "    ${0} [OPTIONS]"
  echo
  echo 'OPTIONS:'
  echo '    --singleton          Run singleton CnosDB Server (with Meta Service)'
  echo '    --clean              Run cargo clean before building CnosDB Server'
  echo
}

while [[ $# -gt 0 ]]; do
  key=${1}
  case ${key} in
  --singleton)
    ARG_MODE='singleton'
    shift 1
    ;;
  --clean)
    ARG_CLEAN=1
    shift 1
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
done

# This script is at $PROJ_DIR/query_server/test/script/start_and_test.sh
PROJ_DIR=$(
  cd $(dirname $0)
  cd ../../..
  pwd
)
CFG_PATH="${PROJ_DIR}/config/config_8902.toml"

TEST_DATA_DIR="/tmp/cnosdb"
 DB_DIR=${TEST_DATA_DIR}"/1001"
 LOG_DIR=${TEST_DATA_DIR}"/logs"
 LOG_PATH=${LOG_DIR}"/start_and_test.data_node.8902.log"
 CARGO_ARGS='--profile test-ci --package main --bin cnosdb'

# -------------------- Start and Test --------------------

echo "=== CnosDB Start and Test ==="
echo "ARG_MODE  = ${ARG_MODE}"
echo "ARG_CLEAN = ${ARG_CLEAN}"
echo "----------------------------------------"

rm -rf ${DB_DIR}
mkdir -p ${LOG_DIR}

echo "= Building CnosDB Server at ${PROJ_DIR}"
if [ ${ARG_CLEAN} -eq 1 ]; then
  cargo clean
fi
cargo build ${CARGO_ARGS}

echo "= Starting CnosDB Server with config: '${CFG_PATH}'."
nohup cargo run ${CARGO_ARGS} -- run -M ${ARG_MODE} --config ${CFG_PATH} >${LOG_PATH} 2>&1 &
PID=$!

trap "echo '= Received Ctrl+C, stopping.'" SIGINT

echo "= Wait for CnosDB Server(pid=${PID}) startup to complete."
while [ -z "$(curl -s ${URL})" ]; do
  if ! kill -0 ${PID} >/dev/null 2>&1; then
    echo "= CnosDB Server(pid=${PID}) startup failed, printing logs."
    cat $LOG_PATH
    exit 1
  fi
  sleep 2
done

# cargo run --package test -- --query-url http://127.0.0.1:8902 --storage-url http://127.0.0.1:7777
function test() {
   echo "= Testing sqllogicaltests" &&
    cargo run --package sqllogicaltests &&
    echo "= Testing e2e_test" &&
    cargo test --package e2e_test --lib -- reliant:: --nocapture
}

test || EXIT_CODE=$?

echo "= Test completed, killing CnosDB Server(pid=${PID}) and cleaning up '${TEST_DATA_DIR}'."
kill ${PID}
rm -rf ${TEST_DATA_DIR}

exit ${EXIT_CODE:-0}
