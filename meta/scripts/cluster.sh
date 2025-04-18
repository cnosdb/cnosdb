#!/bin/bash

### /tmp/cnosdb/        # BASE_DIR
###   meta/             # CLUSTER_DIR
###     1/              # NODE_DIR @node-1
###       config.toml   # NODE_CONFIG
###       nohup.log     # NODE_NOHUP_LOG
###       PID           # NODE_PID
###     2/              # NODE_DIR @node-2
###       config.toml
###       nohup.log
###       PID

function usage() {
  echo 'Start CnosDB Meta-Server Cluster'
  echo
  echo 'USAGE:'
  echo "    ${0} [OPTIONS]"
  echo
  echo 'OPTIONS:'
  echo '    -n | --node-num <INT>  Define number of meta-server nodes, default: 3'
  echo '    --base-dir <PATH>      Define base-directory, default: /tmp/cnosdb'
  echo '    --system-database-replica <INT>'
  echo '                           Config of meta-server: sys_config.system_database_replica'
  echo '    -h, --help             Show this help message'
  echo
}

set -e

## This script is at $PROJ_DIR/meta/scripts/cluster.sh
PROJ_DIR=$(
  cd $(dirname $0)
  cd ../..
  pwd
)

source "${PROJ_DIR}/config/scripts/common.sh"

NODE_NUM=3
BASE_DIR='/tmp/cnosdb'
while [[ $# -gt 0 ]]; do
  case $1 in
  -n | --node-num)
    check_arg_num $1 $# 2
    check_arg_is_int_and_between_0_10 $1 $2
    NODE_NUM=$2
    shift 2
    ;;
  --base-dir)
    check_arg_num $1 $# 2
    BASE_DIR=$2
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

CLUSTER_DIR="${BASE_DIR}/meta"

echo "=== CnosDB Metadata Server ==="
echo "NODE_NUM    = ${NODE_NUM}"
echo "BASE_DIR    = ${BASE_DIR}"
echo "CLUSTER_DIR = ${CLUSTER_DIR}"
echo "----------------------------------------"

cargo build --package meta --bin cnosdb-meta

echo "Killing all running cnosdb-meta"
kill_cnosdb_meta_processes
sleep 3

## Clean up current directory and generate configurations for new instances

echo "Removing old cnosdb-meta data at: ${CLUSTER_DIR}"
rm -rf "${CLUSTER_DIR}"

echo "Creating directories and generating configurations at: ${CLUSTER_DIR}"
mkdir -p "${CLUSTER_DIR}"

## Generate config files.
function generate_config() {
  GEN_CONFIG=${PROJ_DIR}/config/scripts/generate_config.sh
  local id=$1
  mkdir -p "${CLUSTER_DIR}/${id}"
  $GEN_CONFIG --type meta --id $id --output "${CLUSTER_DIR}/${id}/config.toml" \
    --system-database-replica 1
}

for ((i = 1; i <= $NODE_NUM; i++)); do
  generate_config $i
done

echo "Starting $NODE_NUM cnosdb-meta servers..."

function nohup_run_cnosdb_meta() {
  CNOSDB_META=${PROJ_DIR}/target/debug/cnosdb-meta
  local id=$1
  echo "Starting cnosdb-meta server $id..."
  nohup $CNOSDB_META --config "${CLUSTER_DIR}/${id}/config.toml" >"${CLUSTER_DIR}/${id}/nohup.log" 2>&1 &
  echo $! >"${CLUSTER_DIR}/${id}/PID"
}

for ((i = 1; i <= $NODE_NUM; i++)); do
  nohup_run_cnosdb_meta $i
  sleep 1
done
sleep 5

echo "Initializing server 1 as a single-node cluster"
http_post 8901 /init '{}'
echo "Server 1 joined the single-node cluster as leader"
sleep 3

function add_learner_node() {
  local id=$1
  local addr="127.0.0.1:$2"
  http_post 8901 /add-learner "[${id}, \"${addr}\"]"
}

if [ $NODE_NUM -gt 1 ]; then
  echo "Adding other servers as learners, to receive logs from cluster"
  META_MEMBER_IDS="[1"
  for ((i = 2; i <= $NODE_NUM; i++)); do
    echo "Adding server ${i} to cluster"
    add_learner_node $i $(expr 8901 + \( $i - 1 \) \* 10)
    sleep 3
    META_MEMBER_IDS+=",${i}"
  done
  META_MEMBER_IDS+="]"
  sleep 3

  echo "Changing cluster membership from [1] to ${META_MEMBER_IDS}, make them as voters"
  http_post 8901 /change-membership "${META_MEMBER_IDS}"
  sleep 1
  echo "Cluster membership changed to ${META_MEMBER_IDS}"
fi

echo "Get metrics from the leader"
http_post 8901 /metrics

echo "CnosDB Meta Server Cluster is running ......"
