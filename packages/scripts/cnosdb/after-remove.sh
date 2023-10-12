#!/bin/bash

function disable_systemd {
    systemctl disable cnosdb
    rm -f /lib/systemd/system/cnosdb.service
}

function disable_update_rcd {
    update-rc.d -f cnosdb remove
    rm -f /etc/init.d/cnosdb
}

function disable_chkconfig {
    chkconfig --del cnosdb
    rm -f /etc/init.d/cnosdb
}

if [[ -f /etc/redhat-release ]]; then
    # RHEL-variant logic
    if [[ "$1" = "0" ]]; then
        # CnosDB is no longer installed, remove from init system
        rm -f /etc/default/cnosdb

        if command -v systemctl &>/dev/null; then
            disable_systemd
        else
            # Assuming sysv
            disable_chkconfig
        fi
    fi
elif [[ -f /etc/lsb-release ]]; then
    # Debian/Ubuntu logic
    if [[ "$1" != "upgrade" ]]; then
        # Remove/purge
        rm -f /etc/default/cnosdb

        if command -v systemctl &>/dev/null; then
            disable_systemd
        else
            # Assuming sysv
            disable_update_rcd
        fi
    fi
fi