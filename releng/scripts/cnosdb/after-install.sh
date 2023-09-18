#!/bin/bash

BIN_DIR=/usr/bin
DATA_DIR=/var/lib/cnosdb
LOG_DIR=/var/log/cnosdb
SCRIPT_DIR=/usr/lib/cnosdb/scripts
LOGROTATE_DIR=/etc/logrotate.d

function install_init {
    cp -f $SCRIPT_DIR/init.sh /etc/init.d/cnosdb
    chmod +x /etc/init.d/cnosdb
}

function install_systemd {
    cp -f $SCRIPT_DIR/cnosdb.service /lib/systemd/system/cnosdb.service
    systemctl enable cnosdb
}

function install_update_rcd {
    update-rc.d cnosdb defaults
}

function install_chkconfig {
    chkconfig --add cnosdb
}

# Add defaults file, if it doesn't exist
if [[ ! -f /etc/default/cnosdb ]]; then
    touch /etc/default/cnosdb
fi

# Remove legacy symlink, if it exists
if [[ -L /etc/init.d/cnosdb ]]; then
    rm -f /etc/init.d/cnosdb
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