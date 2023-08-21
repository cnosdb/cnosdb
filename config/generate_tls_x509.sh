#!/usr/bin/env bash

set -xe

DIR='tls'

rm -rf $DIR
mkdir -p $DIR

######
## Generate certificate & certificate request files
######

## The root CA
openssl req -x509 -batch -noenc \
    -newkey rsa:4096 -sha256 -days 3650 \
    -keyout $DIR/ca.key \
    -out $DIR/ca.crt \
    -subj "/CN=the ca" \

# openssl asn1parse -in $DIR/ca.crt -out $DIR/ca.der

openssl req -batch -noenc \
    -newkey rsa:2048 -sha256 \
    -keyout $DIR/server.key \
    -out $DIR/server.req \
    -subj "/CN=the server"

# openssl rsa -in $DIR/server.key -out $DIR/server.rsa

openssl req -batch -noenc \
    -newkey rsa:2048 -sha256 \
    -keyout $DIR/client.key \
    -out $DIR/client.req \
    -subj "/CN=the client"

# openssl rsa -in $DIR/client.key -out $DIR/client.rsa

######
## Convert the certificate requests into self-signed certificates.
######

openssl x509 -req \
    -in $DIR/server.req \
    -out $DIR/server.crt \
    -CA $DIR/ca.crt \
    -CAkey $DIR/ca.key \
    -sha256 -days 3650 \
    -set_serial 22 \
    -extensions v3_server -extfile generate_tls_x509.cnf

rm $DIR/server.req

openssl x509 -req \
    -in $DIR/client.req \
    -out $DIR/client.crt \
    -CA $DIR/ca.crt \
    -CAkey $DIR/ca.key \
    -sha256 -days 3650 \
    -set_serial 33 \
    -extensions v3_client -extfile generate_tls_x509.cnf

rm $DIR/client.req
