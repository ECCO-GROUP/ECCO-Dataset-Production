#!/usr/bin/env bash
#---------------------------------------------------------------------
#
# Name: docker_aws_push.sh
#
# Utility script to push ECCO Dataset Production images to AWS ECR.
#
# Usage: ./docker_aws_push.sh
#
#---------------------------------------------------------------------

# value here can overrride .env settings:
#export VERSION=1.0.0

COMPOSE_FILE='../docker-compose.aws.yaml'

cd "$(dirname "$0")"

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

# not really used here but defined nonetheless to avoid Docker compose
# warning messages:
if [ -z "$BUILD_PLATFORM" ]; then
    export BUILD_PLATFORM=linux/amd64
    echo "using BUILD_PLATFORM: ${BUILD_PLATFORM}"
else
    echo "using BUILD_PLATFORM from environment: ${BUILD_PLATFORM}"
fi

# ensure that the target repos exist:
repos=$(docker compose -f ${COMPOSE_FILE} config | grep 'image:' | sed s/image:.*\\/// | sed s/:latest// )

for repo in ${repos}; do
    if ! aws ecr describe-repositories --repository-names ${repo} &>/dev/null; then
        echo "repository ${repo} does not exist; creating..."
        aws ecr create-repository --repository-name ${repo} &>/dev/null
        echo "...done."
    fi
done

# authenticate to AWS ECR...:
aws ecr get-login-password | docker login --username AWS --password-stdin ${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com

# ...and push:
for service in $(docker compose -f ${COMPOSE_FILE} config --services); do
    echo "pushing ${service} (this might take awhile)..."
    docker compose -f ${COMPOSE_FILE} push -q ${service}
    echo "...done"
done
