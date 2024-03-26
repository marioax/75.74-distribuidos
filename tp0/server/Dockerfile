FROM python:3.9.7-slim

RUN mkdir -p /build
WORKDIR /build
COPY ./server/config.ini /
COPY ./server .

RUN python -m unittest tests/test_common.py
ENTRYPOINT ["/bin/sh"]
