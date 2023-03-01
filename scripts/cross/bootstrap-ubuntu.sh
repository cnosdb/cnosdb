#!/bin/sh
set -o errexit

apt-get update
apt-get install -y wget git unzip gnupg apt-transport-https

cat <<-EOF > /etc/apt/sources.list.d/llvm.list
deb http://apt.llvm.org/xenial/ llvm-toolchain-xenial-12 main
deb-src http://apt.llvm.org/xenial/ llvm-toolchain-xenial-12 main
EOF

wget -O - https://apt.llvm.org/llvm-snapshot.gpg.key| apt-key add -
apt-get update
apt-get install -y libclang1-12
apt-get install -y llvm-12


dpkg --add-architecture arm64
apt-get update
apt-get install -y libssl-dev libssl-dev:arm64 zlib1g-dev zlib1g-dev:arm64
