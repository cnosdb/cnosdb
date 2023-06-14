#!/usr/bin/env bash

# ./generate-metadata.sh <result_path> <source> <source_id> <dataset>
# Write system and comment into ${RESULT_PATH} file

set -e

#yq --version

RESULT_PATH=$1
source=$2
source_id=$3
dataset=$4

case ${source} in
pr)
    jq '.system = "CnosDB(PR#'${source_id}')"' <${RESULT_PATH} >${RESULT_PATH}.tmp && mv ${RESULT_PATH}.tmp ${RESULT_PATH}
    ;;
release)
    jq '.system = "CnosDB(Release@'${source_id}')"' <${RESULT_PATH} >${RESULT_PATH}.tmp && mv ${RESULT_PATH}.tmp ${RESULT_PATH}
    ;;
*)
    echo "Unsupported benchmark source: ${source}"
    exit 1
    ;;
esac
jq '.comment = "TODO"' <${RESULT_PATH} >${RESULT_PATH}.tmp && mv ${RESULT_PATH}.tmp ${RESULT_PATH}
