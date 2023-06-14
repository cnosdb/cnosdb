#!/bin/bash -e

# from: https://github.com/ClickHouse/ClickBench/blob/main/generate-results.sh

# ./update-results.sh <dataset> <results_dir>
# Use all *.json files under ./results/${dataset}/ as a result to construct a new html file and put it in ./results/${dataset}.html

dataset=$1
results_dir=${2:-"./results"}

if [ -z "$dataset" ]; then
    echo "Usage: $0 <dataset>"
    exit 1
fi

(
    sed '/^const data = \[$/q' ${dataset}/index.html.template

    FIRST=1
    cp /home/gitlab-runner/benchmark_results/${dataset}/*.json ${results_dir}/${dataset}/
    find "${results_dir}/${dataset}/" -name '*.json' | while read -r file; do
        [[ $file =~ ^(hardware|versions)/ ]] && continue

        [ "${FIRST}" = "0" ] && echo -n ','
        jq --compact-output ". += {\"source\": \"${file}\"}" "${file}"
        FIRST=0
    done

    echo ']; // end of data'
    sed '0,/^\]; \/\/ end of data$/d' ${dataset}/index.html.template

) >${results_dir}/${dataset}.html
