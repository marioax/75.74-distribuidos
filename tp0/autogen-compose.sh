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
%s
    networks:
      - testing_net
    depends_on:
      - server
"

bet_template=" \
     - NAME=%s
      - SURNAME=%s
      - DNI=%d
      - BIRTH=%s
      - NUM=%d
"

env_bet_generator() {
    local names=("PEDRO" "SANTIAGO" "MARTIN" "JUAN" "ALFREDO")
    local surnames=("RODRIGUEZ" "PEREZ" "GARCIA" "LOPEZ" "MORALES")

    local name=${names[(RANDOM % ${#names[@]})]}
    local surname=${surnames[(RANDOM % ${#surnames[@]})]}
    local dni=$((15000000 + 1000 * (RANDOM % 45000000))) 
    local birth="1999-03-17" 
    local num=$RANDOM

    local bet=$(printf "$bet_template" "$name" "$surname" $dni "$birth" $num) 
    printf "$bet"
}

clients=""

for i in $(seq 1 $1); do
    bet=$(env_bet_generator) 
    clients+=$(printf "$cli_template\n" $i $i $i $i "$bet");
done 

printf "$compose" "$clients" > $2

