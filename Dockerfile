FROM cnosdb/cnosdb-build as build

# This environment variable is set by Github Actions.
#
# Unlike the 'git clone' command, Github CI won't generate directory '.git' when coping source code
# for git commands to use, so here uses docker build-arg to fetch the commit hash.
ARG git_hash
ENV CNOSDB_GIT_HASH = $git_hash

# Build
COPY . /cnosdb
RUN cd /cnosdb && cargo build --release --package main --bin cnosdb \
    && cargo build --release --package client --bin cnosdb-cli \
    && cargo build --release --package meta --bin cnosdb-meta

FROM centos

ENV RUST_BACKTRACE 1

COPY --from=build /cnosdb/target/release/cnosdb /usr/bin/cnosdb
COPY --from=build /cnosdb/target/release/cnosdb-cli /usr/bin/cnosdb-cli
COPY --from=build /cnosdb/target/release/cnosdb-meta /usr/bin/cnosdb-meta

COPY ./config/config.toml /etc/cnosdb/cnosdb.conf
