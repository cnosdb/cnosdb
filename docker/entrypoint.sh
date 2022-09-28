#!/bin/bash
set -e

if [ "$1" = 'cnosdb' ]; then
   cargo run -- run --cpu 4 --memory 64
fi
