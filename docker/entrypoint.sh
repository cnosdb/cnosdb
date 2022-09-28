#!/bin/bash
set -e

if [ "$1" = 'cnosdb' ]; then
   cargo run -- run --cpu 4 --memory 64
fi

#if [ "$1" = 'Ubuntu' ]; then
#	 apt update \
#   && apt install --yes pkg-config openssl libssl-dev g++ cmake git \
#   && git clone https://github.com/google/flatbuffers.git \
#   && cd flatbuffers \
#   && cmake -G "Unix Makefiles" -DCMAKE_BUILD_TYPE=Release \
#   && make install \
#   && cargo build
#fi
#
#if [ "$1" = 'Debian' ]; then
#	 apt update \
#   && apt install --yes pkg-config openssl libssl-dev g++ cmake git \
#   && git clone https://github.com/google/flatbuffers.git \
#   && cd flatbuffers \
#   && cmake -G "Unix Makefiles" -DCMAKE_BUILD_TYPE=Release \
#   && make install \
#   && cargo build
#fi
#
#if [ "$1" = 'Archlinux' ]; then
#	pacman -Syu \
#  && pacman -S --yes pkg-config openssl libssl-dev g++ cmake git flatbuffers \
#  && cargo build
#fi
#
#if [ "$1" = 'macOS' ]; then
#	brew update \
#	&& mkdir /Users/xiaominghao/eldhewfdug \
#  && brew install pkg-config openssl libssl-dev g++ cmake git flatbuffers \
#  && cargo build
#fi