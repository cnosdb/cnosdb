FROM cnosdb/cnosdb-build as build

# Build
COPY . /cnosdb
RUN cd /cnosdb && cargo build --release --bin main

FROM cnosdb/alpine-glibc

ENV RUST_BACKTRACE 1

COPY --from=build /cnosdb/target/release/main /usr/bin/cnosdb

COPY ./config/config.toml /etc/cnosdb/cnosdb.conf

ENTRYPOINT /usr/bin/cnosdb run --cpu ${cpu} --memory ${memory} --config /etc/cnosdb/cnosdb.conf