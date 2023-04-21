#!/usr/bin/env bash

set -e

current_dir=$(dirname "$0")

cat ${current_dir}/sql/queries.sql
