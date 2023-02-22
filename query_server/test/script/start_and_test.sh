#!/usr/bin/env bash
set -e

# define environment
export HTTP_HOST=${HTTP_HOST:-"127.0.0.1:31001"}
export URL="http://${HTTP_HOST}/api/v1/ping"
source "$HOME/.cargo/env"

SINGLETON=0
while [[ $# -gt 0 ]]; do
  key=${1}
  case ${key} in
  --singleton)
    SINGLETON=1
    shift 1
    ;;
  *)
    usage
    exit 1
    ;;
  esac
done

function start_cnosdb() {
    rm -rf ./data
    if [ -e "./target/test-ci/cnosdb" ];then
      nohup ./target/test-ci/cnosdb run --config ./config/config_31001.toml > /tmp/cnosdb/logs/start_and_test.data_node.31001.log 2>&1&
    else
      nohup cargo run --profile test-ci -- run --config ./config/config_31001.toml > /tmp/cnosdb/logs/start_and_test.data_node.31001.log 2>&1&
    fi
    echo $!
}

function start_cnosdb_singleton() {
    rm -rf ./data
    mkdir -p /tmp/cnosdb/logs/
    if [ -e "./target/release/cnosdb" ];then
      nohup ./target/release/cnosdb singleton --config ./config/config_31001.toml > /tmp/cnosdb/logs/start_and_test.data_node.31001.log 2>&1&
    else
      nohup cargo run --release -- singleton --config ./config/config_31001.toml > /tmp/cnosdb/logs/start_and_test.data_node.31001.log 2>&1&
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
    cargo test --package e2e_test
}

echo "Starting cnosdb"

PID=0
if [ ${SINGLETON} -eq 0 ]; then
    PID=$(start_cnosdb)
else
    PID=$(start_cnosdb_singleton)
fi

echo "Wait for pid=${PID} startup to complete"

(wait_start && test) || EXIT_CODE=$?

echo "Test complete, killing ${PID}"

kill ${PID}

exit ${EXIT_CODE:-0}
