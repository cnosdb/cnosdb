FROM ghcr.io/cross-rs/x86_64-unknown-linux-gnu:0.2.4-centos

COPY scripts/cross/bootstrap-centos.sh scripts/cross/entrypoint-centos.sh scripts/cross/pre-install.sh /
RUN /bootstrap-centos.sh && bash /pre-install.sh

ENTRYPOINT [ "/entrypoint-centos.sh" ]
