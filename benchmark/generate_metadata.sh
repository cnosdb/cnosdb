#!/usr/bin/env bash

# ./generate-metadata.sh <result_path> <source> <source_id> <dataset>
# Write system and comment into ${RESULT_PATH} file

set -e

yq --version

RESULT_PATH=$1
source=$2
source_id=$3
dataset=$4

case ${source} in
pr)
    yq -i '.system = "CnosDB(PR#'${source_id}')"' ${RESULT_PATH}
    ;;
release)
    yq -i '.system = "CnosDB(Release@'${source_id}')"' ${RESULT_PATH}
    ;;
*)
    echo "Unsupported benchmark source: ${source}"
    exit 1
    ;;
esac
yq -i '.comment = "TODO"' ${RESULT_PATH}
