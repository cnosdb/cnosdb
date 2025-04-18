#!/usr/bin/env bash

set -xe

## This script is at $PROJ_DIR/config/scripts/generate_tls_x509.sh
PROJ_DIR=$(
  cd $(dirname $0)
  cd ..
  pwd
)

TLS_DIR="${PROJ_DIR}/config/resource/tls"
echo "TLS_DIR: $TLS_DIR"
rm -rf $TLS_DIR
mkdir -p $TLS_DIR

######
## Generate certificate & certificate request files
######

## The root CA
openssl req -x509 -batch -noenc \
    -newkey rsa:4096 -sha256 -days 3650 \
    -keyout $TLS_DIR/ca.key \
    -out $TLS_DIR/ca.crt \
    -subj "/CN=the ca" \

# openssl asn1parse -in $TLS_DIR/ca.crt -out $TLS_DIR/ca.der

openssl req -batch -noenc \
    -newkey rsa:2048 -sha256 \
    -keyout $TLS_DIR/server.key \
    -out $TLS_DIR/server.req \
    -subj "/CN=the server"

# openssl rsa -in $TLS_DIR/server.key -out $TLS_DIR/server.rsa

openssl req -batch -noenc \
    -newkey rsa:2048 -sha256 \
    -keyout $TLS_DIR/client.key \
    -out $TLS_DIR/client.req \
    -subj "/CN=the client"

# openssl rsa -in $TLS_DIR/client.key -out $TLS_DIR/client.rsa

######
## Convert the certificate requests into self-signed certificates.
######

openssl x509 -req \
    -in $TLS_DIR/server.req \
    -out $TLS_DIR/server.crt \
    -CA $TLS_DIR/ca.crt \
    -CAkey $TLS_DIR/ca.key \
    -sha256 -days 3650 \
    -set_serial 22 \
    -extensions v3_server -extfile generate_tls_x509.cnf

rm $TLS_DIR/server.req

openssl x509 -req \
    -in $TLS_DIR/client.req \
    -out $TLS_DIR/client.crt \
    -CA $TLS_DIR/ca.crt \
    -CAkey $TLS_DIR/ca.key \
    -sha256 -days 3650 \
    -set_serial 33 \
    -extensions v3_client -extfile generate_tls_x509.cnf

rm $TLS_DIR/client.req
