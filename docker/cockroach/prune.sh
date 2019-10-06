#!/bin/bash

docker system prune -a -f --volumes --filter "label=maintainer=tjveil@gmail.com"