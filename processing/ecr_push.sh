#!/bin/bash
# More information here: https://docs.aws.amazon.com/AmazonECR/latest/userguide/getting-started-cli.html
# Follows commands listed in "Push commands for {repository_name}" when viewing the repository in ECR

repository_name=$1
tag=$2
ecco_version=$3
region=us-west-2

# Retrieve an authentication token and authenticate Docker client to the registry
aws --profile saml-pub ecr get-login-password --region $region | docker login --username AWS --password-stdin 448078824696.dkr.ecr.$region.amazonaws.com

# Build the Docker image from the local directory. Image will be called $repository_name:$tag
docker build . -f ./src/lambda_code/$ecco_version/Dockerfile --tag $repository_name:$tag

# Tag the Docker image in order to push it to the repository
docker tag $repository_name:$tag 448078824696.dkr.ecr.$region.amazonaws.com/$repository_name:$tag

# Push the image to the AWS ECR repository
docker push 448078824696.dkr.ecr.$region.amazonaws.com/$repository_name:$tag