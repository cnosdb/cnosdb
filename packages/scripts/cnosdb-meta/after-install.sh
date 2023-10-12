#!/bin/bash

BIN_DIR=/usr/bin
DATA_DIR=/var/lib/cnosdb
LOG_DIR=/var/log/cnosdb
SCRIPT_DIR=/usr/lib/cnosdb-meta/scripts

function install_init {
    cp -f $SCRIPT_DIR/init.sh /etc/init.d/cnosdb-meta
    chmod +x /etc/init.d/cnosdb-meta
}

function install_systemd {
    cp -f $SCRIPT_DIR/cnosdb-meta.service /lib/systemd/system/cnosdb-meta.service
    systemctl enable cnosdb-meta
}

function install_update_rcd {
    update-rc.d cnosdb-meta defaults
}

function install_chkconfig {
    chkconfig --add cnosdb-meta
}

# Add defaults file, if it doesn't exist
if [[ ! -f /etc/default/cnosdb-meta ]]; then
    touch /etc/default/cnosdb-meta
fi

# Remove legacy symlink, if it exists
if [[ -L /etc/init.d/cnosdb-meta ]]; then
    rm -f /etc/init.d/cnosdb-meta
fi

# Distribution-specific logic
if [[ -f /etc/redhat-release ]]; then
    # RHEL-variant logic
    if command -v systemctl &>/dev/null; then
        install_systemd
    else
        # Assuming sysv
        install_init
        install_chkconfig
    fi
elif [[ -f /etc/debian_version ]]; then
    chown -R -L cnosdb:cnosdb $LOG_DIR
    chown -R -L cnosdb:cnosdb $DATA_DIR
    chmod 755 $LOG_DIR
    chmod 755 $DATA_DIR

    # Debian/Ubuntu logic
    if command -v systemctl &>/dev/null; then
        install_systemd
    else
        # Assuming sysv
        install_init
        install_update_rcd
    fi
fi