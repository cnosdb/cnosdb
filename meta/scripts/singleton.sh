#!/bin/bash

function usage {
  echo 'Start CnosDB Meta-Server Singleton'
  echo
  echo 'USAGE:'
  echo "    ${0} [OPTIONS]"
  echo
  echo 'OPTIONS:'
  echo '    --base-dir <PATH>      Define base-directory, default: /tmp/cnosdb'
  echo '    --node_id <INT>        Config of meta-server global.node_id'
  echo '    --init                 Initialize meta-server if --node_id specified'
  echo '    -h, --help             Show this help message'
  echo
}

set -e

## This script is at $PROJ_DIR/meta/scripts/singleton.sh
PROJ_DIR=$(
  cd $(dirname $0)
  cd ../..
  pwd
)

source "${PROJ_DIR}/config/scripts/common.sh"

BASE_DIR='/tmp/cnosdb'
NODE_ID=0
INIT=0
while [[ $# -gt 0 ]]; do
  case $1 in
  --base-dir)
    check_arg_num $1 $# 2
    BASE_DIR=$2
    shift 2
    ;;
  --node-id)
    check_arg_num $1 $# 2
    check_arg_is_int_and_between_0_10 $1 $2
    NODE_ID=$2
    shift 2
    ;;
  --init)
    INIT=1
    shift 1
    ;;
  -h | --help)
    usage
    exit 0
    ;;
  *)
    echo "Unknown option '$1'"
    exit 1
    ;;
  esac
done

## If --node-id not specified, start and initialize.
if [[ $NODE_ID -eq 0 ]]; then
  ${PROJ_DIR}/meta/scripts/cluster.sh --node-num 1 --base-dir ${BASE_DIR}
  exit $?
fi

LISTEN_PORT=$(expr 8901 + \( $NODE_ID - 1 \) \* 10)
CLUSTER_DIR="${BASE_DIR}/meta"
SERVER_DIR="${CLUSTER_DIR}/${NODE_ID}"
mkdir -p "${CLUSTER_DIR}"

echo "=== CnosDB Metadata Server ==="
echo "BASE_DIR    = ${BASE_DIR}"
echo "CLUSTER_DIR = ${CLUSTER_DIR}"
echo "SERVER_DIR  = ${SERVER_DIR}"
echo "LISTEN_PORT = ${LISTEN_PORT}"
echo "NODE_ID     = ${NODE_ID}"
echo "INIT        = ${INIT}"
echo "----------------------------------------"

cargo build --package meta --bin cnosdb-meta

## Generate config file.
GEN_CONFIG=${PROJ_DIR}/config/scripts/generate_config.sh
mkdir -p ${SERVER_DIR}
$GEN_CONFIG --type meta --id $NODE_ID --output "${SERVER_DIR}/config.toml" \
  --system-database-replica 1

## Start server.
echo "Starting cnosdb-meta server ${NODE_ID}..."
CNOSDB_META=${PROJ_DIR}/target/debug/cnosdb-meta
nohup $CNOSDB_META --config "${SERVER_DIR}/config.toml" >"${SERVER_DIR}/nohup.log" 2>&1 &
echo $! >"${SERVER_DIR}/PID"

sleep 5

## If --node-id specified, start, and initialize if --init specified.
if [[ $INIT -ne 0 ]]; then
  http_post $(expr 8901 + \( $NODE_ID - 1 \) \* 10) /init '{}'
fi
