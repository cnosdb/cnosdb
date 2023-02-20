#!/bin/bash -e

/usr/bin/cnosdb run -config /etc/cnosdb/cnosdb.conf $CNOSDB_OPTS &
PID=$!
echo $PID > /var/run/cnosdb/cnosdb.pid

## TODO Verify that the service exists.

echo "CnosDB started"