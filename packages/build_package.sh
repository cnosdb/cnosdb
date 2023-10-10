#!/bin/bash
set -e

NAME=""
BUILD_OPTION=""
VERSION=""
TARGET=""

USER=cnosdb
GROUP=cnosdb
DESCRIPTION="An Open Source Distributed Time Series Database with high performance, high compression ratio and high usability."
LICENSE="AGPL-3.0"
VENDOR="CnosDB Tech (Beijing) Limited"
MAINTAINER="CnosDB Team"
WEBSITE="https://www.cnosdb.com/"
LOG_DIR="/var/log/cnosdb"
DATA_DIR="/var/lib/cnosdb"

# Print help
usage() {
  cat <<EOF
Usage: $0 -n <package-name> -v <version> [[-t <target>] [-b <BUILD_OPTION>] [-h]]

Build and upload packages.

Options:
  -n <package-name>   The name of the package to build. Required.
  -v <version>        The version of the package to build. Required.
  -t <target>         The binary of the target. Required.
  -b <BUILD_OPTION>   Build option [latest, nightly, release]. Optional. Default is release.
  -h                  Show this help message.
EOF
}

# Parsing command-line options and parameters
while getopts "n:v:t:b:h" opt; do
  case ${opt} in
  n) NAME=$OPTARG ;;
  v) VERSION=${OPTARG#v} ;;
  t) TARGET=$OPTARG ;;
  b) BUILD_OPTION=$OPTARG ;;
  h)
    usage
    exit 0
    ;;
  \?)
    echo "Invalid option: -$OPTARG" >&2
    usage
    exit 1
    ;;
  :)
    echo "Option -$OPTARG requires an argument." >&2
    usage
    exit 1
    ;;
  esac
done

# Verify if necessary parameters exist
if [ -z "$NAME" ]; then
  echo "Package name is missing! Use -n option to specify the package name."
  usage
  exit 1
fi

# Verify if the build options are legal
if [ "$BUILD_OPTION" != "latest" ] && [ "$BUILD_OPTION" != "nightly" ] && [ "$BUILD_OPTION" != "release" ]; then
  echo "Build option is invalid! Use -b option to specify the build option."
  usage
  exit 1
fi

# If BUILD_OPTION is not equal to release, then use nightly or latest directly
if [ "$BUILD_OPTION" != "release" ]; then
  VERSION="$BUILD_OPTION"
fi

# Obtain the base name of the current Git Repo
get_repo_name() {
  local repo_path=$(git rev-parse --show-toplevel 2>/dev/null)
  if [ -n "$repo_path" ]; then
    echo $(basename "$repo_path")
  else
    echo ""
  fi
}

repo_name=$(get_repo_name)

# According to repo_name Set Version Variable
set_version_based_on_repo() {
  if [ "$repo_name" = "cnosdb" ]; then
    VERSION=${VERSION}"-community"
  elif [ "$repo_name" = "cnosdb-enterprise" ]; then
    VERSION=${VERSION}"-enterprise"
  else
    echo "Not a valid repo."
    exit 1
  fi
}

set_version_based_on_repo

# Build FPM commands and package them
build_fpm_cmd() {
  local name="$1"
  local arch="$2"
  local output_type="$3"
  local target="$4"
  local pkg_temp=$(mktemp -d)

  # Create packaging layout
  mkdir -p "${pkg_temp}/usr/bin" \
    "${pkg_temp}/var/log/cnosdb" \
    "${pkg_temp}/var/lib/cnosdb" \
    "${pkg_temp}/etc/cnosdb" \
    "${pkg_temp}/usr/lib/${name}/scripts"

  # Copy Service Script
  cp "./packages/scripts/${name}/init.sh" "${pkg_temp}/usr/lib/${name}/scripts/init.sh"
  chmod 0644 "${pkg_temp}/usr/lib/${name}/scripts/init.sh"
  cp "./packages/scripts/${name}/${name}.service" "${pkg_temp}/usr/lib/${name}/scripts/${name}.service"
  chmod 0644 "${pkg_temp}/usr/lib/${name}/scripts/${name}.service"

  if [ "${name}" == "cnosdb" ]; then
    cp ./config/config.toml "${pkg_temp}/etc/${name}/${name}.conf"

    # Copy Binary Files
    cp "./target/${target}/release/cnosdb" "${pkg_temp}/usr/bin/cnosdb"
    cp "./target/${target}/release/cnosdb-cli" "${pkg_temp}/usr/bin/cnosdb-cli"

    chmod 755 "${pkg_temp}/usr/bin/cnosdb"
    chmod 755 "${pkg_temp}/usr/bin/cnosdb-cli"
  elif [ "${name}" == "cnosdb-meta" ]; then
    cp ./meta/config/config.toml "${pkg_temp}/etc/cnosdb/${name}.conf"
    cp "./target/${target}/release/cnosdb-meta" "${pkg_temp}/usr/bin/cnosdb-meta"
    chmod 755 "${pkg_temp}/usr/bin/cnosdb-meta"
  else
    echo "Invalid build name."
  fi

  chmod 0644 "${pkg_temp}/etc/cnosdb/${name}.conf"

  # Build the package
  fpm -t "${output_type}" \
    -C "${pkg_temp}" \
    -n "${name}" \
    -v "${VERSION}" \
    --architecture "${arch}" \
    -s dir \
    --url "${WEBSITE}" \
    --before-install ./packages/scripts/"${name}"/before-install.sh \
    --after-install ./packages/scripts/"${name}"/after-install.sh \
    --after-remove ./packages/scripts/"${name}"/after-remove.sh \
    --directories "${LOG_DIR}" \
    --directories "${DATA_DIR}" \
    --rpm-attr 755,${USER},${GROUP}:${LOG_DIR} \
    --rpm-attr 755,${USER},cnosdb:"${DATA_DIR}" \
    --config-files /etc/cnosdb/${name}.conf \
    --maintainer "${MAINTAINER}" \
    --vendor "${VENDOR}" \
    --license "${LICENSE}" \
    --description "${DESCRIPTION}" \
    --iteration 1
}

main() {
  ARCH=""

  if [[ ${TARGET} == "x86_64-unknown-linux-gnu" ]]; then
    ARCH="x86_64"
  elif [[ ${TARGET} == "aarch64-unknown-linux-gnu" ]]; then
    ARCH="arm64"
  else
    echo "Unknown target: $TARGET"
  fi

  output_types=("deb" "rpm")
  for output_type in "${output_types[@]}"; do

    build_fpm_cmd "${NAME}" "${ARCH}" "${output_type}" "${TARGET}"
  done
}

main
