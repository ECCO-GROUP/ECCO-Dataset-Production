#!/bin/bash
set -eo pipefail

./ECCO-Dataset-Production/docker/docker_aws_build.sh
./ECCO-Dataset-Production/docker/docker_aws_push.sh
