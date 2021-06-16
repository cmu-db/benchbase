# Basic CockroachDB Cluster with HAProxy
Simple 3 node CockroachDB cluster with HAProxy acting as load balancer

## Services
* `crdb-0` - CockroachDB node
* `crdb-1` - CockroachDB node
* `crdb-2` - CockroachDB node
* `lb` - HAProxy acting as load balancer

## Getting started
1) run `docker-compose up` or `./up.sh`
2) visit the CockroachDB UI @ http://localhost:8080
3) visit the HAProxy UI @ http://localhost:8081
4) have fun!

## Helpful Commands

### Open Interactive Shells
```bash
docker-compose exec crdb-0 /bin/bash
docker-compose exec crdb-1 /bin/bash
docker-compose exec crdb-2 /bin/bash
docker-compose exec lb /bin/sh
```

### Stop Individual nodes
```bash
docker-compose stop crdb-0
docker-compose stop crdb-1
docker-compose stop crdb-2
```