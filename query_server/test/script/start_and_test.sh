#!/usr/bin/env bash
set -e

# define environment
export HTTP_HOST=${HTTP_HOST:-"127.0.0.1:31001"}
export URL="http://${HTTP_HOST}/api/v1/ping"
source "$HOME/.cargo/env"

function start_cnosdb() {
    rm -rf ./data
    if [ -e "./target/release/cnosdb" ];then
      nohup ./target/release/cnosdb run --config ./config/config_31001.toml > /tmp/cnosdb/logs/start_and_test.data_node.31001.log 2>&1&
    else
      nohup cargo run --release -- run --config ./config/config_31001.toml > /tmp/cnosdb/logs/start_and_test.data_node.31001.log 2>&1&
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

PID=$(start_cnosdb)

echo "Wait for pid=${PID} startup to complete"

(wait_start && test) || EXIT_CODE=$?

echo "Test complete, killing ${PID}"

kill ${PID}

exit ${EXIT_CODE:-0}