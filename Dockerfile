FROM cnosdb/cnosdb-build as build

# Build
COPY . /cnosdb
RUN cd /cnosdb && cargo build --release --package main --bin cnosdb \
    && cargo build --release --package client --bin cnosdb-cli \
    && cargo build --release --package meta --bin cnosdb-meta

FROM cnosdb/alpine-glibc

ENV RUST_BACKTRACE 1

COPY --from=build /cnosdb/target/release/cnosdb /usr/bin/cnosdb
COPY --from=build /cnosdb/target/release/cnosdb-cli /usr/bin/cnosdb-cli
COPY --from=build /cnosdb/target/release/cnosdb-meta /usr/bin/cnosdb-meta

COPY ./config/config.toml /etc/cnosdb/cnosdb.conf

ENTRYPOINT /usr/bin/cnosdb run --cpu ${cpu} --memory ${memory} --config /etc/cnosdb/cnosdb.conf