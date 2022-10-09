ARG RUST_VERSION=1.62.1
FROM rust:${RUST_VERSION}-slim-bullseye as build

RUN apt update \
    && apt install --yes pkg-config openssl libssl-dev g++ cmake git

RUN git clone -b v2.0.6 --depth 1 https://github.com/google/flatbuffers.git && cd flatbuffers \
    && cmake -G "Unix Makefiles" -DCMAKE_BUILD_TYPE=Release \
    && make install

COPY . /cnosdb
WORKDIR /cnosdb

RUN cargo build --release --bin main

FROM ubuntu:focal

ENV RUST_BACKTRACE 1

COPY --from=build /cnosdb/target/release/main /usr/bin/cnosdb

COPY ./docker/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh
COPY ./config/config.toml /etc/cnosdb/cnosdb.conf

ENTRYPOINT ["/entrypoint.sh"]