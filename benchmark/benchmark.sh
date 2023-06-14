#!/usr/bin/env bash

set -e

current_dir=$(dirname "$0")

source ${current_dir}/shell_env.sh

set +x

echo "###############################################"
echo "Running benchmark..."

echo "Checking script dependencies..."
# OpenBSD netcat do not have a version arg
# nc --version
bc --version
jq --version
#yq --version

RESULT_DIR=${RESULT_DIR}/${BENCHMARK_DATASET}
export RESULT_PATH=${RESULT_DIR}/${BENCHMARK_ID}.json

mkdir -p ${RESULT_DIR}

# Load benchmark dataset env
source ${current_dir}/${BENCHMARK_DATASET}/shell_env.sh

################# Prepare data #################
echo "Loading data..."
RES_METRICS_AND_TIME=$(${current_dir}/${BENCHMARK_DATASET}/prepare_data.sh)
data_size=$(echo ${RES_METRICS_AND_TIME} | awk '{print $1}' | bc -l)
load_time=$(echo ${RES_METRICS_AND_TIME} | awk '{print $2}' | bc -l)
echo "Data loaded ${data_size} in ${load_time}."

echo '{}' >${RESULT_PATH}
jq ".load_time = ${load_time} | .data_size = ${data_size} | .result = []" <${RESULT_PATH} >${RESULT_PATH}.tmp && mv ${RESULT_PATH}.tmp ${RESULT_PATH}

################# Generate result metadata #################
echo '{}' >${RESULT_PATH}
# ./generate-metadata.sh <result_path> <source> <source_id> <dataset>
${current_dir}/generate_metadata.sh ${RESULT_PATH} ${BENCHMARK_TYPE} ${BENCHMARK_ID} ${BENCHMARK_DATASET}
jq ".date = \"$(date -u +%Y-%m-%d)\"" <${RESULT_PATH} >${RESULT_PATH}.tmp && mv ${RESULT_PATH}.tmp ${RESULT_PATH}
jq ".machine = \"${MACHINE}\"" <${RESULT_PATH} >${RESULT_PATH}.tmp && mv ${RESULT_PATH}.tmp ${RESULT_PATH}
jq '.cluster_size = 1' <${RESULT_PATH} >${RESULT_PATH}.tmp && mv ${RESULT_PATH}.tmp ${RESULT_PATH}
jq '.tags = ["Rust", "time-series"]' <${RESULT_PATH} >${RESULT_PATH}.tmp && mv ${RESULT_PATH}.tmp ${RESULT_PATH}
jq ".load_time = ${load_time} | .data_size = ${data_size} | .result = []" <${RESULT_PATH} >${RESULT_PATH}.tmp && mv ${RESULT_PATH}.tmp ${RESULT_PATH}

################# Generate queries #################
echo "Generating queries..."
${current_dir}/${BENCHMARK_DATASET}/generate_queries.sh

################# Run queries #################
echo "Running queries..."
${current_dir}/${BENCHMARK_DATASET}/run_queries.sh
