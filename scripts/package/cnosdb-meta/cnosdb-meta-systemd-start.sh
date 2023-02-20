#!/bin/bash -e

/usr/bin/cnosdb-meta --config /etc/cnosdb/cnosdb-meta.conf $CNOSDB_OPTS &
PID=$!
echo $PID > /var/run/cnosdb/cnosdb-meta.pid

## TODO Verify that the service exists.

echo "CnosDB meta service started"