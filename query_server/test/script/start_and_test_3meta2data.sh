#!/usr/bin/env bash
set -e
# Modefied default config
sed -i "s/1000000000/10/g" ./run_cluster.sh
sed -i "s/build/& --release/g" ./run_cluster.sh
sed -i "s/build/& --release/g" ./meta/cluster.sh
sed -i "s/debug/release/g" ./run_cluster.sh
sed -i "s/debug/release/g" ./meta/cluster.sh

# Start CnosDB cluster
bash ./run_cluster.sh
# kill all cnosdb
killdb() {
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
# kill all cnosdb-meta
killmeta() {
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
# test case
function test() {
    echo "Testing query/test" && \
    cargo run --package test && \
    echo "Testing e2e_test" && \
    cargo test --package e2e_test && \
    cargo run --package sqllogicaltests
}


echo "Start testing"
test 
echo "Test complete, killing CnosDB Server,clean data"
(killdb && killmeta && rm -rf /tmp/cnosdb)