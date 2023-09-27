#!/bin/bash

hostname=$(hostname -a)

if [[ "$hostname" == *"meta"* ]]; then
  echo "This hostname contains 'meta'"
  sed "s/0.0.0.0/$hostname.meta/g" /etc/config/config.toml > /etc/config.toml
  nodeid=${hostname##*-}
  nodeid=$(($nodeid+200))
  sed -i "s/nodeid/$nodeid/g" /etc/config.toml
  /usr/bin/cnosdb-meta --config /etc/config.toml
fi

if [[ "$hostname" == *"tskv"* ]]; then
  echo "This hostname contains 'tskv'"
  sed "s/hostname/$hostname.tskv/g" /etc/config/config.toml > /etc/config.toml
  nodeid=${hostname##*-}
  nodeid=$(($nodeid+1000))
  sed -i "s/nodeid/$nodeid/g" /etc/config.toml
  /usr/bin/cnosdb run --config /etc/config.toml
fi

if [[ "$hostname" == *"query"* ]]; then
  echo "This hostname contains 'query'"
  sed "s/hostname/$hostname.query/g" /etc/config/config.toml > /etc/config.toml
  nodeid=${hostname##*-}
  nodeid=$(($nodeid+1001))
  sed -i "s/nodeid/$nodeid/g" /etc/config.toml
  /usr/bin/cnosdb run --config /etc/config.toml
fi
