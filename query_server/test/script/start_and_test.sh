#!/usr/bin/env bash
set -e

# define environment
export HTTP_HOST=${HTTP_HOST:-"127.0.0.1:8902"}
export URL="http://${HTTP_HOST}/api/v1/ping"
source "$HOME/.cargo/env"

function usage() {
  echo "Start CnosDB Server and run tests, you may need to start CnosDB Meta Server first."
  echo
  echo "USAGE:"
  echo "    ${0} [OPTIONS]"
  echo
  echo "OPTIONS:"
  echo "    --singleton          Run singleton CnosDB Server (with Meta Service)"
  echo
}

CFG_PATH="./config/config_8902.toml"
EXE_PATH="./target/test-ci/cnosdb"

TEST_DATA_DIR="/tmp/cnosdb"
DB_DIR=${TEST_DATA_DIR}"/1001"
LOG_DIR=${TEST_DATA_DIR}"/logs"
LOG_PATH=${LOG_DIR}"/start_and_test.data_node.8902.log"

EXE_RUN_CMD=${EXE_PATH}" run"
CARGO_RUN_CMD="cargo run --profile test-ci -- run"

while [[ $# -gt 0 ]]; do
  key=${1}
  case ${key} in
  --singleton)
    EXE_RUN_CMD=${EXE_RUN_CMD}" --deployment-mode singleton"
    CARGO_RUN_CMD=${CARGO_RUN_CMD}" --deployment-mode singleton"
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

function start_cnosdb() {
  if [ -e ${EXE_PATH} ];then
    # nohup ./target/test-ci/cnosdb run --config ./config/config_8902.toml > /tmp/cnosdb/logs/start_and_test.data_node.8902.log 2>&1&
    nohup ${EXE_RUN_CMD} --config ${CFG_PATH} > ${LOG_PATH} 2>&1&
  else
    # nohup cargo run --profile test-ci -- run --config ./config/config_8902.toml > /tmp/cnosdb/logs/start_and_test.data_node.8902.log 2>&1&
    nohup ${CARGO_RUN_CMD} --config ${CFG_PATH} > ${LOG_PATH} 2>&1&
  fi
  echo $!
}

function wait_start() {
    while [ "$(curl -s ${URL})" == "" ] && kill -0 ${PID}; do
        sleep 2;
    done
}

function test() {
    echo "Testing query/test" && \
    cargo run --package test && \
    echo "Testing e2e_test" && \
    cargo test --package e2e_test && \
    cargo run --package sqllogicaltests
}

echo "Starting cnosdb"

rm -rf ${DB_DIR}
mkdir -p ${LOG_DIR}

PID=$(start_cnosdb)

echo "Wait for CnosDB Server(pid=${PID}) startup to complete"
trap "echo 'Received Ctrl+C, stopping.'" SIGINT

(wait_start && test) || EXIT_CODE=$?

echo "Test complete, killing CnosDB Server(pid=${PID})"

kill ${PID}

exit ${EXIT_CODE:-0}
