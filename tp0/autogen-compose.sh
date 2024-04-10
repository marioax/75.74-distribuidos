#!/bin/bash

# Generates a docker compose for spawn `n` clients
# @args:
#   - $1: number of clients 
#   - $2: output file 

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
      - SERVER_ID=0
      - CLI_NUM=%d
    volumes:
      - ./server/config.ini:/config.ini
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
    volumes:
      - ./client/config.yaml:/config.yaml
      - ./.data/agency-%d.csv:/bets.csv
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
    clients+=$(printf "$cli_template\n" $i $i $i $i);
done 

printf "$compose" $1 "$clients" > $2

