FROM golang:1.17.5 as builder
WORKDIR /go/src/github.com/cnosdatabase/cnosdb
COPY . /go/src/github.com/cnosdatabase/cnosdb

# Proxy, You know.
# RUN go env -w GOPROXY=https://goproxy.cn,direct

RUN go install ./...

FROM debian:stretch
COPY --from=builder /go/bin/* /usr/bin/
COPY --from=builder /go/src/github.com/cnosdatabase/cnosdb/etc/config.sample.toml /etc/cnosdb/cnosdb.conf

EXPOSE 8086
VOLUME /var/lib/cnosdb

COPY docker/entrypoint.sh /entrypoint.sh
COPY docker/init-cnosdb.sh /init-cnosdb.sh
RUN chmod +x /entrypoint.sh /init-cnosdb.sh
ENTRYPOINT ["/entrypoint.sh"]
CMD ["cnosdb"]