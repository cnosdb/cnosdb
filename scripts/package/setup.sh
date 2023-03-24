#!/bin/bash

set -e

# Check if it is the root directory of the git project
if [ ! -d ".git" ]; then
  echo "Must be executed in the root directory of the Git project."
  exit 1
fi

# Install cross
cargo install cross

# Clean cargo build cache
cargo clean

# Build cnosdb
cross build --release --workspace --bins --target x86_64-unknown-linux-gnu
cross build --release --workspace --bins --target aarch64-unknown-linux-gnu

docker-compose -f ./scripts/package/docker-compose.yml up