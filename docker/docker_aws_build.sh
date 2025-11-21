#!/usr/bin/env bash
#---------------------------------------------------------------------
#
# Name: docker_aws_build.sh
#
# Utility script to build AWS version of ECCO Dataset Production
# containers.
#
# Usage: ./docker_aws_build.sh
#
#---------------------------------------------------------------------

cd "$(dirname "$0")"

COMPOSE_FILE='../docker-compose.aws.yaml'

if ! command -v aws &>/dev/null; then
    echo 'AWS CLI has not been installed; see https://aws.amazon.com/cli/.'
    exit 1
fi

if [ -z "$AWS_ACCOUNT_ID" ]; then
    export AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
    echo "using AWS_ACCOUNT_ID: ${AWS_ACCOUNT_ID}"
else
    echo "using AWS_ACCOUNT_ID from environment: ${AWS_ACCOUNT_ID}"
fi

if [ -z "$AWS_REGION" ]; then
    export AWS_REGION=$(aws configure get region)
    echo "using AWS_REGION: ${AWS_REGION}"
else
    echo "using AWS_REGION from environment: ${AWS_REGION}"
fi

if [ -z "$BUILD_PLATFORM" ]; then
    export BUILD_PLATFORM=linux/amd64
    echo "using BUILD_PLATFORM: ${BUILD_PLATFORM}"
else
    echo "using BUILD_PLATFORM from environment: ${BUILD_PLATFORM}"
fi

for service in $(docker compose -f ${COMPOSE_FILE} config --services); do
    docker compose -f ${COMPOSE_FILE} build ${service}
done
