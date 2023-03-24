FROM ubuntu:focal

ARG DEBIAN_FRONTEND=noninteractive
ENV TZ=Asia/Shanghai

RUN apt-get update && apt-get install ruby-full binutils wget  apt-utils rpm -y
RUN gem install fpm

WORKDIR /cnosdb

ENTRYPOINT: ["/bin/bash","-c"]