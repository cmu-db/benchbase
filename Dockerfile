# A simple dev environment container for benchbase.

FROM --platform=linux maven:3.8.5-eclipse-temurin-17

RUN addgroup --gid 1000 containergroup \
    && adduser --disabled-password --gecos 'Container User' --uid 1000 --gid 1000 containeruser

USER containeruser
