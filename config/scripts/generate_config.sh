#!/bin/bash

function usage {
  echo 'Generate CnosDB Server & Meta-Server Config'
  echo
  echo 'USAGE:'
  echo "    ${0} [OPTIONS]"
  echo
  echo 'OPTIONS:'
  echo '    --id <INT>                           Define node-id, default: 1'
  echo '    --base-dir <PATH>                    Define base-directory, default: /tmp/cnosdb'
  echo '    -t | --type <data|meta>              Argument of exe: cnosdb-config, default: data'
  echo '    -o | --output <stdout|<PATH>>        Argument of exe: cnosdb-config, default: stdout'
  echo '    --data-deployment-mode <query_tskv|tskv|query|singleton>'
  echo '                                         Config of cnosdb-server: deployment.mode'
  echo '    --data-max-buffer-size <INT>         Config of cnosdb-server: cache.max_buffer_size'
  echo '    --system-database-replica <INT>      Config of cnosdb-server&cnosdb-meta-server:'
  echo '                                         - cnosdb-server:      meta.system_database_replica'
  echo '                                         - cnosdb-meta-server: sys_config.system_database_replica'
  echo '    -h, --help                           Show this help message'
  echo
}

set -e

## This script is at $PROJ_DIR/config/scripts/generate_config.sh
CRATE_DIR=$(
  cd $(dirname $0)
  cd ..
  pwd
)

source "${CRATE_DIR}/scripts/common.sh"

ID=1
BASE_DIR=/tmp/cnosdb
CONFIG_TYPE=data
OUTPUT_TYPE=stdout
while [[ $# -gt 0 ]]; do
  case $1 in
  --id)
    check_arg_num $1 $# 2
    check_arg_is_int_and_between_0_10 $1 $2
    ID=$2
    shift 2
    ;;
  --base-dir)
    check_arg_num $1 $# 2
    BASE_DIR=$2
    shift 2
    ;;
  -t | --type)
    check_arg_num $1 $# 2
    CONFIG_TYPE=$2
    shift 2
    ;;
  -o | --output)
    check_arg_num $1 $# 2
    OUTPUT_TYPE=$2
    shift 2
    ;;
  --data-deployment-mode)
    check_arg_num $1 $# 2
    export CNOSDB_DEPLOYMENT_MODE=$2
    shift 2
    ;;
  --data-max-buffer-size)
    check_arg_num $1 $# 2
    export CNOSDB_CACHE_MAX_BUFFER_SIZE=$2
    shift 2
    ;;
  --system-database-replica)
    check_arg_num $1 $# 2
    ## Export environment variable for cnosdb-server
    export CNOSDB_META_SYSTEM_DATABASE_REPLICA=$2
    ## Export environment variable for cnosdb-meta-server
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

set +e
## This expression may return 0
ID_OFF=$(expr $ID - 1)
set -e

if [[ $CONFIG_TYPE == "data" ]]; then
  # DATA_NODE_ID=$(expr 1001 + $ID_OFF)
  DATA_NODE_ID=$ID
  DATA_NODE_DIR="${BASE_DIR}/data/${DATA_NODE_ID}"
  # DATA_NODE_CONFIG_PATH="${DATA_NODE_DIR}/config/config.toml"
  export CNOSDB_GLOBAL_NODE_ID=$DATA_NODE_ID
  export CNOSDB_GLOBAL_LOCATION="/dc1/rack${ID}"
  export CNOSDB_STORAGE_PATH="${DATA_NODE_DIR}/data"
  export CNOSDB_WAL_PATH="${DATA_NODE_DIR}/wal"
  export CNOSDB_LOG_PATH="${DATA_NODE_DIR}/logs"
  export CNOSDB_SERVICE_HTTP_LISTEN_PORT=$(expr 8902 + $ID_OFF \* 10)
  export CNOSDB_SERVICE_GRPC_LISTEN_PORT=$(expr 8903 + $ID_OFF \* 10)
  export CNOSDB_SERVICE_FLIGHT_RPC_LISTEN_PORT=$(expr 8904 + $ID_OFF \* 10)
  export CNOSDB_SERVICE_TCP_LISTEN_PORT=$(expr 8905 + $ID_OFF \* 10)
  ## External configs for old config files like config/cnosdb_8902.toml
  export CNOSDB_CACHE_PARTITION=16
  export CNOSDB_SUBSCRIPTION_TIMEOUT=300
elif [[ $CONFIG_TYPE == "meta" ]]; then
  META_NODE_ID=$ID
  META_NODE_DIR="${BASE_DIR}/meta/${META_NODE_ID}"
  # META_NODE_CONFIG_PATH="${META_NODE_DIR}/config/config.toml"
  export CNOSDB_META_GLOBAL_NODE_ID=$META_NODE_ID
  export CNOSDB_META_GLOBAL_LISTEN_PORT=$(expr 8901 + $ID_OFF \* 10)
  export CNOSDB_META_GLOBAL_DATA_PATH="${META_NODE_DIR}/data"
  export CNOSDB_META_LOG_PATH="${META_NODE_DIR}/logs"
  ## External configs for old config files like meta/config/cnosdb_8901.toml
  export CNOSDB_META_CLUSTER_HEARTBEAT_INTERVAL=300
else
  echo "Invalid config type $CONFIG_TYPE"
  exit 1
fi

cargo run --quiet --package config -- gen --type $CONFIG_TYPE --output $OUTPUT_TYPE
