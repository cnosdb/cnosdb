#!/bin/bash

function usage() {
  echo 'Start CnosDB Meta-Server'
  echo
  echo 'USAGE:'
  echo "    ${0} [CMD] [OPTIONS]"
  echo
  echo 'COMMANDs:'
  echo '    add         Startup new meta-server and change membership'
  echo '    change      Change membership'
  echo
  echo 'OPTIONS:'
  echo '    --base-dir <PATH>         Define base-directory, default: /tmp/cnosdb'
  echo '    --change-membership <STR> Define new membership, e.g. "[1,2,3]"'
  echo '    --node_id <INT>           Config of meta-server global.node_id'
  echo '    --system-database-replica <INT>'
  echo '                              Config of meta-server: sys_config.system_database_replica'
  echo '    -h, --help                Show this help message'
  echo
}

set -o errexit

## This script is at $PROJ_DIR/meta/tests/test_meta.sh
PROJ_DIR=$(
  cd $(dirname $0)
  cd ../..
  pwd
)

source "${PROJ_DIR}/config/scripts/common.sh"

BASE_DIR='/tmp/cnosdb'
CHANGE_MEMBERSHIP=''
NODE_ID=0
CMD=$1
shift
while [[ $# -gt 0 ]]; do
  case $1 in
  --base-dir)
    check_arg_num $1 $# 2
    BASE_DIR=$2
    shift 2
    ;;
  --change-membership)
    check_arg_num $1 $# 2
    CHANGE_MEMBERSHIP=$2
    shift 2
    ;;
  --node-id)
    check_arg_num $1 $# 2
    check_arg_is_int_and_between_0_10 $1 $2
    NODE_ID=$2
    shift 2
    ;;
  --system-database-replica)
    check_arg_num $1 $# 2
    export CNOSDB_META_SYS_CONFIG_SYSTEM_DATABASE_REPLICA=$2
    shift 2
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

LISTEN_PORT=$(expr 8901 + \( $NODE_ID - 1 \) \* 10)
CLUSTER_DIR="${BASE_DIR}/meta"
SERVER_DIR="${CLUSTER_DIR}/${NODE_ID}"
mkdir -p "${CLUSTER_DIR}"

echo "=== CnosDB Metadata Server ==="
echo "BASE_DIR          = ${BASE_DIR}"
echo "CLUSTER_DIR       = ${CLUSTER_DIR}"
echo "SERVER_DIR        = ${SERVER_DIR}"
echo "CHANGE_MEMBERSHIP = ${CHANGE_MEMBERSHIP}"
echo "NODE_ID           = ${NODE_ID}"
echo "LISTEN_PORT       = ${LISTEN_PORT}"
echo "----------------------------------------"

cargo build --package meta --bin cnosdb-meta

## Generate configurations for new instances

function start_meta_server {
  ## Generate config files.
  GEN_CONFIG=${PROJ_DIR}/config/scripts/generate_config.sh
  mkdir -p ${SERVER_DIR}
  $GEN_CONFIG --type meta --id $NODE_ID --output "${SERVER_DIR}/config.toml" \
    --system-database-replica 1

  echo "Starting cnosdb-meta server ${NODE_ID}..."
  CNOSDB_META=${PROJ_DIR}/target/debug/cnosdb-meta
  nohup $CNOSDB_META --config "${SERVER_DIR}/config.toml" >"${SERVER_DIR}/nohup.log" 2>&1 &
  echo $! >"${SERVER_DIR}/PID"
}

if [ $CMD == "add" ]; then
  start_meta_server
  sleep 5
  http_post 8901 /add-learner "[${NODE_ID}, \"127.0.0.1:${LISTEN_PORT}\"]"
  echo "Node ${NODE_ID} joined cluster as leaner"
  sleep 1
  echo "Changing membership from to ${CHANGE_MEMBERSHIP}"
  echo
  http_post 8901 /change-membership "${CHANGE_MEMBERSHIP}"
  sleep 1
  echo "Membership changed"
  sleep 1
elif [ $CMD == "change" ]; then
  echo "Changing membership to ${CHANGE_MEMBERSHIP}"
  echo
  http_post 8901 /change-membership "${CHANGE_MEMBERSHIP}"
  sleep 1
  echo "Membership changed"
  sleep 1
else
  echo "Unknown command ${CMD}"
  exit 1
fi

echo "Get metrics from the leader"
http_get 8901 /metrics
sleep 1
