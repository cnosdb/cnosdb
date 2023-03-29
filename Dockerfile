FROM cnosdb/cnosdb-build:v2.2.0 as build

# Build
COPY . /cnosdb
RUN cd /cnosdb && cargo build --release --package main --package client --package meta --bins

FROM cnosdb/alpine-glibc

ENV RUST_BACKTRACE 1

COPY --from=build /cnosdb/target/release/cnosdb /usr/bin/cnosdb
COPY --from=build /cnosdb/target/release/cnosdb-cli /usr/bin/cnosdb-cli
COPY --from=build /cnosdb/target/release/cnosdb-meta /usr/bin/cnosdb-meta

COPY ./config/config.toml /etc/cnosdb/cnosdb.conf

ENTRYPOINT /usr/bin/cnosdb run --deployment-mode singleton --cpu ${cpu} --memory ${memory} --config /etc/cnosdb/cnosdb.conf