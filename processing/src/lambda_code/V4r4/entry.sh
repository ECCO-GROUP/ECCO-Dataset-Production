#!/bin/sh

# Prepare AWS Lambda
# This script is called via the Lambda function Dockerfile as the ENTRYPOINT.

# Determines if the image is being run in Lambda or not. Decides whether to invoke
# the function through the Runtime Interface Emulator (RIE) or directly
# if running in Lambda cloud
if [ -z "${AWS_LAMBDA_RUNTIME_API}" ]; then
  exec /usr/local/bin/aws-lambda-rie /usr/local/bin/python3 -m awslambdaric $1
else
  exec /usr/local/bin/python3 -m awslambdaric $1
fi     