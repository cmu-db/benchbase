#!/bin/bash

docker-compose down --remove-orphans --volumes

docker system prune -a -f --volumes --filter "label=maintainer=tjveil@gmail.com"