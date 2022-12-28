#!/usr/bin/env bash
set -e

# define environment
export HTTP_HOST=${HTTP_HOST:-"127.0.0.1:31001"}
export URL="http://${HTTP_HOST}/api/v1/ping"
source "$HOME/.cargo/env"

function start_cnosdb() {
    rm -rf ./data
    if [ -e "./target/release/main" ];then
      nohup ./target/release/main run --cpu 4 --memory 8 --http-host ${HTTP_HOST} > /dev/null 2>&1&
    else
      nohup cargo run --release -- run --cpu 4 --memory 8 --http-host ${HTTP_HOST} > /dev/null 2>&1&
    fi
    echo $!
}

function wait_start() {
    while [ "$(curl -s ${URL})" == "" ] && kill -0 ${PID}; do
        sleep 2s;
    done
}

function test() {
    cargo run --package test && \
    cargo test --package e2e_test
}

echo "Starting cnosdb"

PID=$(start_cnosdb)

echo "Wait for pid=${PID} startup to complete"

(wait_start && test) || EXIT_CODE=$?

echo "Test complete, killing ${PID}"

kill ${PID}

exit ${EXIT_CODE:-0}