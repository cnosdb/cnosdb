#!/bin/bash
set -e

if [ "$1" = 'cnosdb' ]; then
	/init-cnosdb.sh "${@}"
fi

if [ "$1" = 'cnosdb-meta' ]; then
	/init-cnosdb.sh "${@}"
fi

if [ "$1" = 'cnosdb-data' ]; then
	/init-cnosdb.sh "${@}"
fi

exec "$@"
