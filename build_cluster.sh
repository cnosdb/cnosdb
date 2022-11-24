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

cd ./meta
echo "*** run meta cluster ......"
./cluster.sh


cd ..

kill
sleep 1
rm -rf /tmp/cnosdb/1001
rm -rf /tmp/cnosdb/2001

echo "*** build main ......"
cargo build

echo "*** build client ......"
cargo build --package client --bin client

echo "*** start CnosDB server 31001......"
nohup ./target/debug/main run  --config ./config/config_31001.toml > /dev/null &

sleep 1

echo "*** start CnosDB server 32001......"
nohup ./target/debug/main run  --config ./config/config_32001.toml > /dev/null &

echo "\n*** CnosDB Data Server Cluster is running ......"

sleep 1000000000
