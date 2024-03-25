#!/bin/bash

compose="
version: '3.9'
name: tp0
services:
  server:
    container_name: server
    image: server:latest
    entrypoint: python3 /main.py
    environment:
      - PYTHONUNBUFFERED=1
      - LOGGING_LEVEL=DEBUG
    networks:
      - testing_net

  %s

networks:
  testing_net:
    ipam:
      driver: default
      config:
        - subnet: 172.25.125.0/24
"

cli_template="
  client_%d:
    container_name: client_%d
    image: client:latest
    entrypoint: /client
    environment:
      - CLI_ID=%d
      - CLI_LOG_LEVEL=DEBUG
    networks:
      - testing_net
    depends_on:
      - server
"
clients=""

for i in $(seq 1 $1); do
    clients+=$(printf "$cli_template\n" $i $i $i);
done 

printf "$compose" "$clients" > $2

