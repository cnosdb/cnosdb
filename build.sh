#!/bin/bash


HERE=`pwd`

function build_target()
{
    TARGET=$1

    cd $HERE
    cd cmd/$TARGET
    rm $TARGET -rf

    echo `date` go build -gcflags=all="-N -l" -o $TARGET ./main.go
    go build -gcflags=all="-N -l" -o $TARGET ./main.go

    echo `date` cp ./$TARGET /usr/bin -f
    rm /usr/bin/$TARGET -rf
    cp ./$TARGET /usr/bin -f

    echo -e "\n"
    cd $HERE
}


build_target cnosdb
build_target cnosdb-cli
build_target cnosdb-ctl
build_target cnosdb-inspect
build_target cnosdb-meta
build_target cnosdb-tools


