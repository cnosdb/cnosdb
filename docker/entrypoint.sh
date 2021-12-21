#!/bin/bash
set -e

if [ "${1:0:1}" = '-' ]; then
    set -- cnosdb "$@"
fi

if [ "$1" = 'cnosdb' ]; then
	/init-cnosdb.sh "${@:2}"
fi

exec "$@"
