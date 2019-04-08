#!/bin/bash

docker-compose build --no-cache

docker-compose up --no-start
docker-compose start roach-0
docker-compose start roach-1
docker-compose start roach-2
docker-compose start lb

sleep 5

docker-compose exec roach-0 /cockroach/cockroach sql --insecure --execute="CREATE DATABASE oltpbench;"
docker-compose exec roach-0 /cockroach/cockroach sql --insecure --execute="SET CLUSTER SETTING server.remote_debugging.mode = \"any\";"