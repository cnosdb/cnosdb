#!/bin/env bash

DATA_DIR=/var/lib/cnosdb

if ! id cnosdb &>/dev/null; then
    useradd --system -U -M cnosdb -s /bin/false -d $DATA_DIR

fi

if [[ -f /etc/cnosdb/cnosdb-meta.conf ]]; then
	  backup_name="cnosdb-meta.conf.$(date +%s).backup"
	  echo "A backup of your current configuration can be found at: /etc/cnosdb/$backup_name"
	  cp -a /etc/cnosdb/cnosdb-meta.conf /etc/cnosdb/$backup_name
fi