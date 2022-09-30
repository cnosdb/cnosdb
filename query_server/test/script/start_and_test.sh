#!/usr/bin/env bash
set -e

# define environment
export HTTP_HOST=${HTTP_HOST:-"0.0.0.0:31007"}
export URL="http://${HTTP_HOST}/api/v1/ping"

function start_cnosdb() {
    nohup cargo run -- run --cpu 4 --memory 8 --http-host ${HTTP_HOST} > /dev/null 2>&1&
    echo $!
}

function wait_start() {
    while [ "$(curl -s ${URL})" == "" ] && kill -0 ${PID}; do
        sleep 2s;
    done
}

function init() {
    curl -i -u "cnosdb:xx" \
        -XPOST "http://${HTTP_HOST}/api/v1/write?db=public" \
        -d 'test_insert_subquery,ta=a1,tb=b1 fa=1,fb=2 3'

    curl -i -u "cnosdb:xx" \
        -XPOST "http://${HTTP_HOST}/api/v1/write?db=public" \
        -d 'test,ta=a1,tb=b1 fa=1,fb=2 3'
}

function test() {
    cargo run --package test
}

echo "Starting cnosdb"

PID=$(start_cnosdb)

echo "Wait for pid=${PID} startup to complete"

(wait_start && init && test) || EXIT_CODE=$?

echo "Test complete, killing ${PID}"

kill ${PID}

exit ${EXIT_CODE:-0}