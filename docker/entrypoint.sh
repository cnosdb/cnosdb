#!/bin/bash
set -e

if [ "$1" = 'Ubuntu' ]; then
	exec apt update \
       apt install --yes pkg-config openssl libssl-dev g++ cmake git \
       git clone https://github.com/google/flatbuffers.git \
       cd flatbuffers \
       cmake -G "Unix Makefiles" -DCMAKE_BUILD_TYPE=Release \
       make install \
       cargo build
fi

if [ "$1" = 'Debian' ]; then
	exec apt update \
       apt install --yes pkg-config openssl libssl-dev g++ cmake git \
       git clone https://github.com/google/flatbuffers.git \
       cd flatbuffers \
       cmake -G "Unix Makefiles" -DCMAKE_BUILD_TYPE=Release \
       make install \
       cargo build
fi

if [ "$1" = 'Archlinux' ]; then
	exec pacman -Syu \
       pacman -S --yes pkg-config openssl libssl-dev g++ cmake git flatbuffers \
       cargo build
fi

if [ "$1" = 'macOS' ]; then
	exec brew update \
       brew install --yes pkg-config openssl libssl-dev g++ cmake git flatbuffers \
       cargo build
fi
