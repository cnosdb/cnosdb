#! /usr/bin/env bash
set -o errexit -o verbose

# shellcheck disable=SC2155
readonly TMP_DIR="$(mktemp -d -p "cnosdb_dep")"
trap 'rm -rf "${TMP_DIR?}"' EXIT

get_platform() {
  local os
  os=$(uname)
  if [[ "${os}" == "Darwin" ]]; then
    echo "osx"
  elif [[ "${os}" == "Linux" ]]; then
    echo "linux"
  else
    >&2 echo "unsupported os: ${os}" && exit 1
  fi
}

get_arch() {
  local os
  local arch
  os=$(uname)
  arch=$(uname -m)
  if [[ "${os}" == "Darwin" && "${arch}" == "arm64" ]]; then
    echo "aarch_64"
  elif [[ "${os}" == "Linux" && "${arch}" == "aarch64" ]]; then
    echo "aarch_64"
  else
    echo "${arch}"
  fi
}

install_protoc() {
  local version=$1
  local install_path=$2

  local base_url="https://github.com/protocolbuffers/protobuf/releases/download"
  local url
  url="${base_url}/v${version}/protoc-${version}-$(get_platform)-$(get_arch).zip"
  local download_path="${TMP_DIR}/protoc.zip"

  echo "Downloading ${url}"
  curl -fsSL "${url}" -o "${download_path}"

  unzip -qq "${download_path}" -d "${TMP_DIR}"
  mv --force --verbose "${TMP_DIR}/bin/protoc" "${install_path}"
}



install_cmake(){
    local version=$1
    local install_path=$2

    local base_url="https://github.com/Kitware/CMake/releases/download"
    local url
    local arch
    arch=$(uname -m)
    url="${base_url}/v${version}/cmake-${version}-$(get_platform)-${arch}.tar.gz"
    local download_path="${TMP_DIR}/cmake.tar.gz"

    echo "Downloading ${url}"
    curl -fsSL "${url}" -o "${download_path}"

    tar -zxvf "${download_path}"
    mv --force --verbose "${TMP_DIR}/cmake-${version}-$(get_platform)-${arch}/bin/cmake" "${install_path}"

}

install_flatbuffer(){
    git clone -b v22.9.29 --depth 1 https://github.com/google/flatbuffers.git
    cd flatbuffers
    cmake -G "Unix Makefiles" -DCMAKE_BUILD_TYPE=Release
    make install -j
}

install_glibc(){
  wget -c https://ftp.gnu.org/gnu/glibc/glibc-2.36.tar.gz
  tar -zxvf glibc-2.36.tar.gz
  cd glibc-2.36
  mkdir -p "${TMP_DIR}/glibc_build" && cd "${TMP_DIR}/glibc_build"
  /glibc-2.36/configure --prefix=/opt/glibc
  make install -j
}


# install_protoc "21.12" "/usr/bin/protoc"
install_cmake "3.25.1" "/usr/bin/cmake"
install_flatbuffer
# install_glibc