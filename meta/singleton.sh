#!/bin/bash

set -o errexit

# This script is at $PROJ_DIR/meta/singleton.sh
PROJ_DIR=$(
  cd $(dirname $0)
  cd ..
  pwd
)
ARG_RUN_RELEASE=0
ARG_SKIP_BUILD=0
ARG_SKIP_CLEAN=0

function usage() {
  echo "Start and initialize CnosDB Metadata Server."
  echo
  echo "USAGE:"
  echo "    ${0} [OPTIONS]"
  echo
  echo "OPTIONS:"
  echo "    --release            Run release version of CnosDB Metadata Server"
  echo "    -sb, --skip-build    Skip building before running CnosdDB Metadata Server"
  echo "    -sc, --skip-clean    Clean data directory before running CnosdDB Metadata Server"
  echo
}

while [[ $# -gt 0 ]]; do
  key=${1}
  case ${key} in
  --release)
    ARG_RUN_RELEASE=1
    shift 1
    ;;
  -sb | --skip-build)
    ARG_SKIP_BUILD=1
    shift 1
    ;;
  -sc | --skip-clean)
    ARG_SKIP_CLEAN=1
    shift 1
    ;;
  -h | --help)
    usage
    exit 0
    ;;
  *)
    usage
    exit 1
    ;;
  esac
done

echo "=== CnosDB Metadata Server (singleton) ==="
echo "ARG_RUN_RELEASE = ${ARG_RUN_RELEASE}"
echo "ARG_SKIP_BUILD  = ${ARG_SKIP_BUILD}"
echo "ARG_SKIP_CLEAN  = ${ARG_SKIP_CLEAN}"
echo "----------------------------------------"

if [ ${ARG_SKIP_BUILD} -eq 0 ]; then
  if [ ${ARG_RUN_RELEASE} -eq 0 ]; then
    cargo build --package meta --bin cnosdb-meta
  else
    cargo build --package meta --bin cnosdb-meta --release
  fi
fi
if [ ${ARG_SKIP_CLEAN} -eq 0 ]; then
  rm -rf /tmp/cnosdb/meta
fi

kill() {
  if [ "$(uname)" = "Darwin" ]; then
    SERVICE='cnosdb-meta'
    if pgrep -xq -- "${SERVICE}"; then
      pkill -9 -f "${SERVICE}"
    fi
  else
    set +e # killall will error if finds no process to kill
    killall cnosdb-meta
    set -e
  fi
}

rpc() {
  local uri=$1
  local body="$2"

  echo '---'" rpc(:$uri, $body)"
  {
    if [ ".$body" = "." ]; then
      curl --silent "127.0.0.1:$uri"
    else
      curl --silent "127.0.0.1:$uri" -H "Content-Type: application/json" -d "$body"
    fi
  } | {
    echo -n '--- '
    if type jq >/dev/null 2>&1; then
      jq
    else
      cat
    fi
  }
  echo
  echo
}

#export RUST_LOG=debug
echo "Killing all running cnosdb-meta"

kill
sleep 3

echo "Start 1 uninitialized cnosdb-meta servers..."

mkdir -p /tmp/cnosdb/logs

if [ ${ARG_RUN_RELEASE} -eq 0 ]; then
  nohup ${PROJ_DIR}/target/debug/cnosdb-meta --config ${PROJ_DIR}/meta/config/config_8901.toml >/tmp/cnosdb/logs/meta_node.1.log &
else
  nohup ${PROJ_DIR}/target/release/cnosdb-meta --config ${PROJ_DIR}/meta/config/config_8901.toml >/tmp/cnosdb/logs/meta_node.1.log &
fi

echo "Server 1 started"
sleep 3

echo "Initialize server 1 as a single-node cluster"
rpc 8901/init '{}'

sleep 1

echo "Get metrics from the leader"
rpc 8901/metrics
