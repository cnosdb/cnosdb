#!/bin/sh

rm -rf /tmp/cnosdb/meta
rm -rf /tmp/cnosdb/1001
rm -rf /tmp/cnosdb/2001

set -o errexit

PROJ_DIR=$(cd `dirname $0`;cd ..;pwd)

cargo build --package meta --bin cnosdb-meta

kill() {
    if [ "$(uname)" = "Darwin" ]; then
        SERVICE='cnosdb-meta'
        if pgrep -xq -- "${SERVICE}"; then
            pkill -f "${SERVICE}"
        fi
    else
        set +e # killall will error if finds no process to kill
        killall cnosdb-meta
        set -e
    fi
}

rpc() {
    local uri=$1
    local body="$2"

    echo '---'" rpc(:$uri, $body)"
    {
        if [ ".$body" = "." ]; then
            curl --silent "127.0.0.1:$uri"
        else
            curl --silent "127.0.0.1:$uri" -H "Content-Type: application/json" -d "$body"
        fi
    } | {
        if type jq > /dev/null 2>&1; then
            jq
        else
            cat
        fi
    }
    echo
    echo
}

#export RUST_LOG=debug
echo "Killing all running cnosdb-meta"

kill
sleep 1

echo "Start 3 uninitialized cnosdb-meta servers..."

mkdir -p /tmp/cnosdb/logs

nohup ${PROJ_DIR}/target/debug/cnosdb-meta  --id 1 --http-addr 127.0.0.1:21001 > /tmp/cnosdb/logs/meta_node.1.log &
echo "Server 1 started"
sleep 1

echo "Initialize server 1 as a single-node cluster"
rpc 21001/init '{}'

echo "Server 1 is a leader now"
sleep 3

echo "Get metrics from the leader"
rpc 21001/metrics
sleep 1

echo "Get metrics from the leader"
rpc 21001/metrics
sleep 1
