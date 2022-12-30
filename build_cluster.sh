#!/bin/sh

kill() {
    if [ "$(uname)" = "Darwin" ]; then
        SERVICE='main'
        if pgrep -xq -- "${SERVICE}"; then
            pkill -f "${SERVICE}"
        fi
    else
        set +e # killall will error if finds no process to kill
        killall main
        set -e
    fi
}

mkdir -p /tmp/cnosdb/logs

cd ./meta
echo "*** run meta cluster ......"
./cluster.sh


cd ..

kill
sleep 1
rm -rf /tmp/cnosdb/1001
rm -rf /tmp/cnosdb/2001
rm -rf /tmp/cnosdb/meta/

echo "*** build main ......"
cargo build

echo "*** build client ......"
cargo build --package client --bin client

echo "*** start CnosDB server 31001......"
nohup ./target/debug/main run  --config ./config/config_31001.toml > /tmp/cnosdb/logs/data_node.1001.log &

sleep 1

echo "*** start CnosDB server 32001......"
nohup ./target/debug/main run  --config ./config/config_32001.toml > /tmp/cnosdb/logs/data_node.2001.log &

echo "\n*** CnosDB Data Server Cluster is running ......"

sleep 1000000000
