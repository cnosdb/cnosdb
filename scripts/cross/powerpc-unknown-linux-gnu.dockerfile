FROM ghcr.io/cross-rs/powerpc-unknown-linux-gnu:0.2.4

COPY scripts/cross/bootstrap-ubuntu.sh scripts/cross/pre-install.sh /
RUN /bootstrap-ubuntu.sh && bash /pre-install.sh
