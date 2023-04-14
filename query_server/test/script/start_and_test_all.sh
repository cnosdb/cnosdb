#! /bin/bash 
set -e 

# define environment
export HTTP_HOST=${HTTP_HOST:-"127.0.0.1:31001"}
export URL="http://${HTTP_HOST}/api/v1/ping"
source "$HOME/.cargo/env"
EXE_PATH="./target/test-ci/cnosdb"
META_PATH="./target/test-ci/cnosdb-meta"
CONF_DATA_1="./config/config_31001.toml"
CONF_DATA_2="./config/config_32001.toml"
CONF_META_1="./meta/config/config_21001.toml"
CONF_META_2="./meta/config/config_21002.toml"
CONF_META_3="./meta/config/config_21003.toml"
DATA_PATH="/tmp/cnosdb"
LOG_PATH=${DATA_PATH}/log
DN_1_LOG_PATH=${LOG_PATH}/data_node_31001.log
DN_2_LOG_PATH=${LOG_PATH}/data_node_32001.log
MT_1_LOG_PATH=${LOG_PATH}/meta_node_21001.log
MT_2_LOG_PATH=${LOG_PATH}/meta_node_21002.log
MT_3_LOG_PATH=${LOG_PATH}/meta_node_21003.log


function usage() {
  echo "Start CnosDB Server and run test, Including several common deployment methods"
  echo
  echo "USAGE:"
  echo "    ${0} [OPTIONS]"
  echo
  echo "OPTIONS:"
  echo "    singleton                      Run singleton CnosDB Server (with Meta Service)"
  echo "    3meta2data                       Run 3meta 2data CnosDB Server                  "
  echo "    1meta2data                       Run 1meta 2data CnosDB Server                  "
  echo "    1meta1data                       Run 1meta 1data CnosDB Server                  "
  echo "    3meta_2data_1query_1tskv         Run 3meta 2data CnosDB Server,Separation of deposit and calculation  "
}

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
        if type jq > /dev/null 2>&1; then
            jq
        else
            cat
        fi
    }
    echo
    echo
}
# test case
function test() {
    echo "Testing query/test" && \
    cargo run --package test && \
    echo "Testing e2e_test" && \
    cargo test --package e2e_test && \
    cargo run --package sqllogicaltests
}

function clean_env(){
  killdb
  killmeta
  rm -rf /tmp/cnosdb
}

function build(){
  cargo build --profile test-ci --package main --bin cnosdb
  cargo build --profile test-ci --package meta --bin cnosdb-meta
  cargo build --profile test-ci --package client --bin cnosdb-cli
}
function 1m1d(){
  nohup ${META_PATH} --config ${CONF_META_1} > ${MT_1_LOG_PATH} &2>1 &
  sleep 3
  rpc 21001/init '{}'
  sleep 1
  rpc 21001/metrics
  nohup ${EXE_PATH} run --config ${CONF_DATA_1} > ${DN_1_LOG_PATH} &2>1 &
}
function 3m2d_sep(){
  nohup ${META_PATH} --config ${CONF_META_1} > ${MT_1_LOG_PATH} &2>1 &
  nohup ${META_PATH} --config ${CONF_META_2} > ${MT_2_LOG_PATH} &2>1 &
  nohup ${META_PATH} --config ${CONF_META_3} > ${MT_3_LOG_PATH} &2>1 &
  sleep 3
  rpc 21001/init '{}'
  sleep 1
  rpc 21001/add-learner       '[2, "127.0.0.1:21002"]'
  sleep 1
  rpc 21001/add-learner       '[3, "127.0.0.1:21003"]'
  sleep 1
  rpc 21001/change-membership '[1, 2, 3]'
  sleep 1
  rpc 21001/metrics
  nohup ${EXE_PATH} run -M query  --config ${CONF_DATA_1} > ${DN_1_LOG_PATH} &2>1 &
  nohup ${EXE_PATH} run -M tskv --config ${CONF_DATA_2} > ${DN_2_LOG_PATH} &2>1 &
}

function 3m2d(){
  nohup ${META_PATH} --config ${CONF_META_1} > ${MT_1_LOG_PATH} &2>1 &
  nohup ${META_PATH} --config ${CONF_META_2} > ${MT_2_LOG_PATH} &2>1 &
  nohup ${META_PATH} --config ${CONF_META_3} > ${MT_3_LOG_PATH} &2>1 &
  sleep 3
  rpc 21001/init '{}'
  sleep 1
  rpc 21001/add-learner       '[2, "127.0.0.1:21002"]'
  sleep 1
  rpc 21001/add-learner       '[3, "127.0.0.1:21003"]'
  sleep 1
  rpc 21001/change-membership '[1, 2, 3]'
  sleep 1
  rpc 21001/metrics
  nohup ${EXE_PATH} run --config ${CONF_DATA_1} > ${DN_1_LOG_PATH} &2>1 &
  nohup ${EXE_PATH} run --config ${CONF_DATA_2} > ${DN_2_LOG_PATH} &2>1 &
}
function 1m2d(){
  nohup ${META_PATH} --config ${CONF_META_1} > ${MT_1_LOG_PATH} &2>1 &
  sleep 3
  rpc 21001/init '{}'
  sleep 1
  rpc 21001/metrics
  nohup ${EXE_PATH} run --config ${CONF_DATA_1} > ${DN_1_LOG_PATH} &2>1 &
  nohup ${EXE_PATH} run --config ${CONF_DATA_2} > ${DN_2_LOG_PATH} &2>1 &
}
if [[ ${1} == "--help" || ${1} == "-h" ]]
then
  usage
  exit 0
fi
# clean env
clean_env
build
mkdir -p ${LOG_PATH}
if [ $# -eq 0 ]
then
  echo "1Meta 1Data"
  1m1d
fi
flag=0

while [[ $# -gt 0 ]]; do
  key=${1}
  case ${key} in
    singleton)
      nohup ${EXE_PATH} run  -M singleton --config ${CONF_DATA_1} > ${DN_1_LOG_PATH} &2>1 &
    shift 1
    ;;
    3meta2data)
     3m2d
    shift 1
    ;;
   1meta2data)
     1m2d
    shift 1
    ;;
   1meta1data)
     1m1d
    shift 1
    ;;
   3meta_2data_1query_1tskv)
     3m2d_sep
     flag=1
    shift 1
    ;;
  *)
     1m1d
    shift 1
    ;;
  esac
done

if [[ ${flag} -eq 0 ]]
then
  test || EXIT_CODE=$?
  echo "Test complete, killing CnosDB Server "
fi
clean_env
exit ${EXIT_CODE}