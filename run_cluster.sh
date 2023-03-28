#!/bin/sh

kill() {
    if [ "$(uname)" = "Darwin" ]; then
        SERVICE='cnosdb'
        if pgrep -xq -- "${SERVICE}"; then
            pkill -f "${SERVICE}"
        fi
    else
        set +e # killall will error if finds no process to kill
        killall cnosdb
        set -e
    fi
}

mkdir -p /tmp/cnosdb/logs

echo "*** run meta cluster ......"
./meta/cluster.sh

kill
sleep 1
rm -rf /tmp/cnosdb/1001
rm -rf /tmp/cnosdb/2001
rm -rf /tmp/cnosdb/meta/

echo "*** build cnosdb ......"
cargo build --package main --bin cnosdb

echo "*** build cnosdb-cli ......"
cargo build --package client --bin cnosdb-cli

echo "*** start CnosDB server 8902......"
nohup ./target/debug/cnosdb run --config ./config/config_8902.toml &> /tmp/cnosdb/logs/data_node.1001.log &

sleep 1

echo "*** start CnosDB server 8912......"
nohup ./target/debug/cnosdb run --config ./config/config_8912.toml &> /tmp/cnosdb/logs/data_node.2001.log &

echo "\n*** CnosDB Data Server Cluster is running ......"

sleep 1000000000
