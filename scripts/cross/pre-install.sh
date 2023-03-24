#!/usr/bin/env bash

set -o errexit -o verbose

readonly TMP_DIR=$(mktemp -d)

get_platform() {
local os=$(uname)
case "$os" in
Darwin) echo "osx" ;;
Linux) echo "linux" ;;
*) echo >&2 "unsupported os: ${os}" && exit 1 ;;
esac
}

get_arch() {
  local os=$(uname)
  local arch=$(uname -m)
  if [[ "$os" == "Darwin" && "$arch" == "arm64" ]]; then
    echo "aarch_64"
  elif [[ "$os" == "Linux" && "$arch" == "aarch64" ]]; then
    echo "aarch_64"
  else
    echo "${arch}"
  fi
}

install_protoc() {
  local version=$1
  local install_path=$2

  local base_url="https://github.com/protocolbuffers/protobuf/releases/download"
  local url="${base_url}/v${version}/protoc-${version}-$(get_platform)-$(get_arch).zip"
  local download_path="${TMP_DIR}/protoc.zip"

  echo "Downloading ${url}"
  curl -fsSL "${url}" -o "${download_path}"

  unzip -qq "${download_path}" -d "${TMP_DIR}"
  chmod 755 "${TMP_DIR}/bin/protoc"
  mv -fv "${TMP_DIR}/bin/protoc" "${install_path}"
  mv -fv "${TMP_DIR}/include/google" "/usr/include"
}

install_cmake() {
  local version=$1
  local install_path=$2

  local base_url="https://github.com/Kitware/CMake/releases/download"
  local url="${base_url}/v${version}/cmake-${version}-$(get_platform)-$(uname -m).tar.gz"
  local download_path="${TMP_DIR}/cmake.tar.gz"

  echo "Downloading ${url}"
  curl -fsSL "${url}" -o "${download_path}"

  tar -xzf "${download_path}" -C "${TMP_DIR}"
  mv -fv "${TMP_DIR}/cmake-${version}-$(get_platform)-$(uname -m)/bin/cmake" "${install_path}"
}

install_flatbuffer() {
    git clone -b v22.9.29 --depth 1 https://github.com/google/flatbuffers.git ${TMP_DIR}/flatbuffers
    cd ${TMP_DIR}/flatbuffers
    sed -i '/-Werror=unused-parameter/d' CMakeLists.txt
    cmake -G "Unix Makefiles" -DCMAKE_BUILD_TYPE=Release
    make install -j
}

install_protoc "3.18.1" "/usr/bin/protoc"
install_cmake "3.23.2" "/usr/bin/cmake"
install_flatbuffer

rm -rf "${TMP_DIR}"