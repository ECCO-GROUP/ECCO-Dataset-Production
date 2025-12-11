#!/usr/bin/env bash
#---------------------------------------------------------------------
#
# Name: docker_dev_build.sh
#
# Utility script to build local (development) ECCO Dataset Production
# containers.
#
# Usage: ./docker_dev_build.sh
#
#---------------------------------------------------------------------

# value here can overrride .env settings:
#export VERSION=1.0.0

COMPOSE_FILE='../docker-compose.dev.yaml'

cd "$(dirname "$0")"

for service in $(docker compose -f ${COMPOSE_FILE} config --services); do
    docker compose -f ${COMPOSE_FILE} build ${service}
done
