ARG RUST_VERSION=1.68.2
FROM rust:${RUST_VERSION}-slim-bullseye as builder

RUN apt update \
    && apt install --yes pkg-config openssl libssl-dev g++ cmake protobuf-compiler git

RUN git clone -b v22.9.29 --depth 1 https://github.com/google/flatbuffers.git && cd flatbuffers \
    && cmake -G "Unix Makefiles" -DCMAKE_BUILD_TYPE=Release \
    && make install && cd .. && rm -rf flatbuffers


COPY . /cnosdb
WORKDIR /cnosdb

RUN cargo build --release --workspace --bin cnosdb --bin cnosdb-cli

FROM ubuntu:focal

COPY --from=builder /cnosdb/target/release/cnosdb /cnosdb/target/release/cnosdb-cli /usr/bin/

COPY config/config.toml /etc/cnosdb/cnosdb.conf
RUN chmod +x /usr/bin/cnosdb /usr/bin/cnosdb-cli

ENTRYPOINT ["/bin/bash", "-c", "cnosdb run --config /etc/cnosdb/cnosdb.conf \"$@\"", "--"]