#!/bin/bash

### /tmp/cnosdb/        # BASE_DIR
###   data/             # CLUSTER_DIR
###     1/              # NODE_DIR @node-1
###       config.toml   # NODE_CONFIG
###       nohup.log     # NODE_NOHUP_LOG
###       PID           # NODE_PID
###     2/              # NODE_DIR @node-2
###       config.toml
###       nohup.log
###       PID

function usage() {
  echo 'Start CnosDB Server Cluster'
  echo
  echo 'If you want to run a single node, please do not set the number of nodes.'
  echo
  echo 'USAGE:'
  echo "    ${0} [OPTIONS]"
  echo
  echo 'OPTIONS:'
  echo '    -p | --profile <debug|release|test-ci>'
  echo '                            Define cargo run --profile <PROFILE>, default: debug'
  echo '    --query-tskv-num <INT>  Define number of query_tskv nodes, default: 0'
  echo '    --query-num <INT>       Define number of query nodes, default: 0'
  echo '    --tskv-num <INT>        Define number of tskv nodes, default: 0'
  echo '    --base-dir <PATH>       Define base-directory, default: /tmp/cnosdb'
  echo '    --system-database-replica <INT>'
  echo '                            Config of cnosdb-server: meta.system_database_replica'
  echo '    -h | --help      Show this help message'
  echo
}

set -e

## This script is at $PROJ_DIR/main/scripts/run_cluster.sh
PROJ_DIR=$(
  cd $(dirname $0)
  cd ../..
  pwd
)

source "${PROJ_DIR}/config/scripts/common.sh"

PROFILE='debug'
QUERY_TSKV_NODE_NUM=0
QUERY_NODE_NUM=0
TSKV_NODE_NUM=0
BASE_DIR='/tmp/cnosdb'
while [[ $# -gt 0 ]]; do
  case $1 in
  -p | --profile)
    check_arg_num $1 $# 2
    PROFILE=$2
    shift 2
    ;;
  --query-tskv-num)
    check_arg_num $1 $# 2
    check_arg_is_int_and_between_0_10 $1 $2
    QUERY_TSKV_NODE_NUM=$2
    shift 2
    ;;
  --query-num)
    check_arg_num $1 $# 2
    check_arg_is_int_and_between_0_10 $1 $2
    QUERY_NODE_NUM=$2
    shift 2
    ;;
  --tskv-num)
    check_arg_num $1 $# 2
    check_arg_is_int_and_between_0_10 $1 $2
    TSKV_NODE_NUM=$2
    shift 2
    ;;
  --base-dir)
    check_arg_num $1 $# 2
    BASE_DIR=$2
    shift 2
    ;;
  --system-database-replica)
    check_arg_num $1 $# 2
    export CNOSDB_META_SYSTEM_DATABASE_REPLICA=$2
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

CLUSTER_DIR="${BASE_DIR}/data"
SINGLETON=1
if [ $QUERY_TSKV_NODE_NUM -ne 0 ] || [ $QUERY_NODE_NUM -ne 0 ] || [ $TSKV_NODE_NUM -ne 0 ]; then
  SINGLETON=0
fi

echo "=== CnosDB Server ==="
echo "SINGLETON           = ${SINGLETON}"
echo "QUERY_TSKV_NODE_NUM = ${QUERY_TSKV_NODE_NUM}"
echo "QUERY_NODE_NUM      = ${QUERY_NODE_NUM}"
echo "TSKV_NODE_NUM       = ${TSKV_NODE_NUM}"
echo "BASE_DIR            = ${BASE_DIR}"
echo "CLUSTER_DIR         = ${CLUSTER_DIR}"
echo "----------------------------------------"

cargo build --profile $PROFILE --package main --bin cnosdb

kill_cnosdb_processes() {
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

echo "Killing all running cnosdb"
kill_cnosdb_processes
sleep 3

## Clean up current directory and generate configurations for new instances

echo "Removing old cnosdb data at: ${CLUSTER_DIR}"
rm -rf "${CLUSTER_DIR}"

echo "Creating directories and generating configurations at: ${CLUSTER_DIR}"
mkdir -p "${CLUSTER_DIR}"

## Generate config files.
function generate_config() {
  GEN_CONFIG="${PROJ_DIR}/config/scripts/generate_config.sh"
  local id=$1
  local mode=$2
  mkdir -p "${CLUSTER_DIR}/$id"
  $GEN_CONFIG --type data --id $id --output "${CLUSTER_DIR}/${id}/config.toml" \
    --data-deployment-mode $mode
}

if [ $SINGLETON -eq 1 ]; then
  generate_config 1 singleton
else
  ATOMIC_NODE_ID=1
  if [ $QUERY_TSKV_NODE_NUM -gt 0 ]; then
    for ((i = 0; i < $QUERY_TSKV_NODE_NUM; i++)); do
      generate_config $ATOMIC_NODE_ID query_tskv
      ATOMIC_NODE_ID=$(expr $ATOMIC_NODE_ID + 1)
    done
  fi

  if [ $QUERY_NODE_NUM -gt 0 ]; then
    for ((i = 0; i < $QUERY_NODE_NUM; i++)); do
      generate_config $ATOMIC_NODE_ID query
      ATOMIC_NODE_ID=$(expr $ATOMIC_NODE_ID + 1)
    done
  fi

  if [ $TSKV_NODE_NUM -gt 0 ]; then
    for ((i = 0; i < $TSKV_NODE_NUM; i++)); do
      generate_config $ATOMIC_NODE_ID tskv
      ATOMIC_NODE_ID=$(expr $ATOMIC_NODE_ID + 1)
    done
  fi
fi

## Start cnosdb servers.

function nohup_run_cnosdb() {
  CNOSDB="${PROJ_DIR}/target/${PROFILE}/cnosdb"
  local id=$1
  echo "Starting cnosdb server $id..."
  nohup $CNOSDB run --config "${CLUSTER_DIR}/${id}/config.toml" >"${CLUSTER_DIR}/${id}/nohup.log" 2>&1 &
  echo $! >"${CLUSTER_DIR}/${id}/PID"
}

if [ $SINGLETON -eq 1 ]; then
  echo "Starting cnosdb singleton server..."
  nohup_run_cnosdb 1
  sleep 1
else
  ATOMIC_NODE_ID=1
  if [ $QUERY_TSKV_NODE_NUM -gt 0 ]; then
    echo "Starting $QUERY_TSKV_NODE_NUM cnosdb query_tskv servers..."
    for ((i = 0; i < $QUERY_TSKV_NODE_NUM; i++)); do
      nohup_run_cnosdb $ATOMIC_NODE_ID
      ATOMIC_NODE_ID=$(expr $ATOMIC_NODE_ID + 1)
      sleep 1
    done
  fi

  if [ $QUERY_NODE_NUM -gt 0 ]; then
    echo "Starting $QUERY_NODE_NUM cnosdb query servers..."
    for ((i = 0; i < $QUERY_NODE_NUM; i++)); do
      nohup_run_cnosdb $ATOMIC_NODE_ID
      ATOMIC_NODE_ID=$(expr $ATOMIC_NODE_ID + 1)
      sleep 1
    done
  fi

  if [ $TSKV_NODE_NUM -gt 0 ]; then
    echo "Starting $TSKV_NODE_NUM cnosdb tskv servers..."
    for ((i = 0; i < $TSKV_NODE_NUM; i++)); do
      nohup_run_cnosdb $ATOMIC_NODE_ID
      ATOMIC_NODE_ID=$(expr $ATOMIC_NODE_ID + 1)
      sleep 1
    done
  fi
fi

echo "CnosDB Data Server Cluster is running ......"
