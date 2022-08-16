#!/bin/bash
# More information here: https://docs.aws.amazon.com/AmazonECR/latest/userguide/getting-started-cli.html
# Follows commands listed in "Push commands for {container_name}" when viewing the container in ECR

container_name=$1
tag=$2
ecco_version=$3
region=us-west-2

# Retrieve an authentication token and authenticate Docker client to the registry
aws --profile saml-pub ecr get-login-password --region $region | docker login --username AWS --password-stdin 448078824696.dkr.ecr.$region.amazonaws.com

# Build the Docker image
docker build . -f ./src/lambda_code/$ecco_version/Dockerfile --tag $container_name

# Tag the Docker image in order to push it to the repository
docker tag $container_name:$tag 448078824696.dkr.ecr.$region.amazonaws.com/$container_name:$tag

# Push the image to the AWS ECR repository
docker push 448078824696.dkr.ecr.$region.amazonaws.com/$container_name:$tag