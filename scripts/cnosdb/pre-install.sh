#!/bin/bash

if [[ -d /etc/opt/cnosdb ]]; then
    # Legacy configuration found
    if [[ ! -d /etc/cnosdb ]]; then
	# New configuration does not exist, move legacy configuration to new location
	echo -e "Please note, CnosDB's configuration is now located at '/etc/cnosdb' (previously '/etc/opt/cnosdb')."
	mv -vn /etc/opt/cnosdb /etc/cnosdb

	if [[ -f /etc/cnosdb/cnosdb.conf ]]; then
	    backup_name="cnosdb.conf.$(date +%s).backup"
	    echo "A backup of your current configuration can be found at: /etc/cnosdb/$backup_name"
	    cp -a /etc/cnosdb/cnosdb.conf /etc/cnosdb/$backup_name
	fi
    fi
fi
