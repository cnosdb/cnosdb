FROM --platform=$TARGETPLATFORM ubuntu
RUN apt update && apt install -y curl iputils-ping wget ca-certificates git openssl unzip && \
    apt clean && update-ca-certificates

ARG git_hash
ENV CNOSDB_GIT_HASH = $git_hash

ENV RUST_BACKTRACE 1

ARG TARGETPLATFORM
COPY ./$TARGETPLATFORM/cnosdb /usr/bin/cnosdb
COPY ./$TARGETPLATFORM/cnosdb-cli /usr/bin/cnosdb-cli

COPY ./config/config.toml /etc/cnosdb/cnosdb.conf

CMD ["cnosdb", "run", "-M", "singleton"]
