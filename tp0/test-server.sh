#!/bin/bash

# Test an echo server
# @args
#   $1: host
#   $2: port
#   $3: count

if [ "$1" == "-h" ] || [ $# -lt 3 ]; then
    echo "usage: $0 [host] [port] [count]"
    exit
fi

HOST=${1:-localhost}
PORT=${2:-8080}
COUNT=${3:-1}


# init the container and connect to target network
docker container create --name netcat-test --network tp0_testing_net alpine:latest \
    sh -c "apk --no-cache add netcat-openbsd && tail -f /dev/null" > /dev/null
docker container start netcat-test > /dev/null
# oneliner equivalent
# docker run --rm --network tp0_testing_net alpine:latest sh -c "(apk --no-cache add netcat-openbsd > /dev/null) && (echo Hola | sleep 1)"

# send n messages
for i in $(seq 1 $COUNT); do
    MSG=$(tr -dc A-Za-z0-9 </dev/urandom | head -c 13; echo)
    printf "\x1b[1mTEST $i:\x1b[0m sending $MSG...\t"

    RES=$(echo $MSG | docker container exec -i netcat-test nc $HOST $PORT)

    if [ "$MSG" == "$RES" ]; then
        printf "\x1b[1;32mOK\x1b[0m\n"
    else
        printf "\x1b[1;31mERROR\x1b[0m\n"
    fi
done

# cleanup
docker container kill netcat-test > /dev/null
docker container rm -f netcat-test > /dev/null
