#!/bin/bash

function usage {
  echo 'Start CnosDB Server Cluster'
  echo
  echo 'USAGE:'
  echo "    ${0} [OPTIONS]"
  echo
  echo 'OPTIONS:'
  echo '    -d | --detach    Run cluster in detached mode'
  echo '    --one-meta       Start 1 meta server node'
  echo '    --three-meta     Start 3 meta server nodes'
  echo '    -h | --help      Show this help message'
  echo
}

set -e

## This script is at $PROJ_DIR/run_cluster.sh
PROJ_DIR=$(
  cd $(dirname $0)
  pwd
)

DATA_NODE_NUM=3
META_NODE_NUM=3
BASE_DIR='/tmp/cnosdb'
DETACH=0
while [[ $# -gt 0 ]]; do
  case $1 in
  -d | --detach)
    DETACH=1
    shift 1
    ;;
  --one-meta)
    META_NODE_NUM=1
    shift 1
    ;;
  --three-meta)
    META_NODE_NUM=3
    shift 1
    ;;
  -h | --help)
    usage
    exit 0
    ;;
  *)
    echo "Unknown option $1"
    exit 1
    ;;
  esac
done

CLUSTER_DIR="${BASE_DIR}/data"

## Start cnosdb-meta server cluster.
$PROJ_DIR/meta/scripts/cluster.sh --node-num $META_NODE_NUM --base-dir $BASE_DIR

## Start cnosdb server cluster.
$PROJ_DIR/main/scripts/cluster.sh --query-tskv-num 3 --base-dir $BASE_DIR

if [ $DETACH -eq 1 ]; then
  exit 0
fi

echo "Press Ctrl+C to exit from this script ......"
trap "echo 'Received Ctrl+C, stopping.'; exit" SIGINT
sleep 1000000000
