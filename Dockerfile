# A simple dev environment container for benchbase.
#
# - Build:
#
#   docker build -t benchbase .
#
# - Run:
#
#   docker run -it --rm --name basebench -v $PWD:/src benchbase /bin/bash

FROM --platform=linux maven:3.8.5-eclipse-temurin-17

RUN addgroup --gid 1000 containergroup \
    && adduser --disabled-password --gecos 'Container User' --uid 1000 --gid 1000 containeruser

USER containeruser
WORKDIR /src
