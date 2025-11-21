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

cd "$(dirname "$0")"

COMPOSE_FILE='../docker-compose.dev.yaml'

for service in $(docker compose -f ${COMPOSE_FILE} config --services); do
    docker compose -f ${COMPOSE_FILE} build ${service}
done
