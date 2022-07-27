#!/bin/bash
container_name=$1
tag=$2
region=us-west-2

# login to aws
python3 ./src/utils/aws_login/aws-login.py -pub -a arn:aws:iam::448078824696:role/power_user

aws --profile saml-pub ecr get-login-password --region $region | docker login --username AWS --password-stdin 448078824696.dkr.ecr.$region.amazonaws.com
docker build . -f ./src/lambda_code/Dockerfile --tag $container_name
docker tag $container_name:$tag 448078824696.dkr.ecr.$region.amazonaws.com/$container_name:$tag
docker push 448078824696.dkr.ecr.$region.amazonaws.com/$container_name:$tag