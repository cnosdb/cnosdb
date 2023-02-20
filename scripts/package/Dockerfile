FROM ubuntu:focal

ARG DEBIAN_FRONTEND=noninteractive
ENV TZ=Asia/Shanghai

RUN apt-get update && apt-get install ruby-full binutils wget  apt-utils rpm -y
RUN gem install fpm

COPY . /cnosdb

WORKDIR /cnosdb

RUN chmod +x scripts/package/package.sh
RUN scripts/package/package.sh -n cnosdb-meta
RUN scripts/package/package.sh -n cnosdb