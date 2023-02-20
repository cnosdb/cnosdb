#!/bin/bash

function disable_systemd {
    systemctl disable cnosdb-meta
    rm -f /lib/systemd/system/cnosdb-meta.service
}

function disable_update_rcd {
    update-rc.d -f cnosdb-meta remove
    rm -f /etc/init.d/cnosdb-meta
}

function disable_chkconfig {
    chkconfig --del cnosdb-meta
    rm -f /etc/init.d/cnosdb-meta
}

if [[ -f /etc/redhat-release ]]; then
    # RHEL-variant logic
    if [[ "$1" = "0" ]]; then
        # CnosDB is no longer installed, remove from init system
        rm -f /etc/default/cnosdb-meta

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
        rm -f /etc/default/cnosdb-meta

        if command -v systemctl &>/dev/null; then
            disable_systemd
        else
            # Assuming sysv
            disable_update_rcd
        fi
    fi
fi