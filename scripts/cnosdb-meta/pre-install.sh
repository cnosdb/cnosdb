#!/bin/bash

if [[ -d /etc/opt/cnosdb-meta ]]; then
    # Legacy configuration found
    if [[ ! -d /etc/cnosdb-meta ]]; then
	# New configuration does not exist, move legacy configuration to new location
	echo -e "Please note, CnosDB's configuration is now located at '/etc/cnosdb' (previously '/etc/opt/cnosdb')."
	mv -vn /etc/opt/cnosdb-meta /etc/cnosdb-meta

	if [[ -f /etc/cnosdb-meta/cnosdb-meta.conf ]]; then
	    backup_name="cnosdb.conf.$(date +%s).backup"
	    echo "A backup of your current configuration can be found at: /etc/cnosdb/$backup_name"
	    cp -a /etc/cnosdb/cnosdb-meta.conf /etc/cnosdb-meta/$backup_name
	fi
    fi
fi
