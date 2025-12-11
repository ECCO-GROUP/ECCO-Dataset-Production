
## Docker Build Resources

The Dockerfiles and scripts included here support both local, and
cloud-based Docker image builds and can either be run directly
(`docker build ...`) or invoked using the Docker Compose files located
at the repository top level (`docker compose -f ... build ...`). In
addition, the included shell scripts automate calls to Docker Compose
so that building, and pushing, the entire ECCO Dataset Production
container-based toolchain can be accomplished with just one or two
commands:

    # Local Docker image builds:
    $ ./docker_dev_build.sh

    # AWS ECR build...:
    $ ./docker_aws_build.sh
    # ...and push:
    $ ./docker_aws_push.sh

### Contents

Docker-related build files and scripts include:

    ../.env                                 Docker VERSION tag default

    ../docker-compose.aws.yaml              Docker compose file to build and push AWS ECR images
    ../docker-compose.dev.yaml              Docker compose file to build images for local development and testing

    ./Dockerfile.aws.base                   AWS base image Dockerfile
    ./Dockerfile.aws.generate-datasets      AWS dataset generation executable image Dockerfile
    ./Dockerfile.dev.base                   Local development base image Dockerfile
    ./Dockerfile.dev.generate-datasets      Local development dataset generation executable image Dockerfile

    ./docker_aws_build.sh                   Automated AWS Docker image builds
    ./docker_aws_push.sh                    Automated AWS ECR Docker image pushes
    ./docker_dev_build.sh                   Automated local development Docker image builds

    ./entrypoint_edp_generate_datasets.sh   Entrypoint script for edp_generate_dataset calls

### Setup

Other than ensuring the Docker daemon is running (`$ docker version`),
no configuration is necesary for local Docker image builds. `../.env`
can be edited to set a numerical version tag if desired but, if not,
all images will be built using a tag of ":latest".

For [AWS ECR](https://aws.amazon.com/ecr/)-targeted builds it is
assumed that an AWS account with appropriate privileges has been
established, and that the AWS Command Line Interface
([CLI](https://aws.amazon.com/cli/)) has been locally installed. In
addition, the following environment variables apply:

- `AWS_ACCOUNT_ID` (no default)
- `AWS_REGION` (no default)
- `BUILD_PLATFORM` (default: `linux/amd64`)

If not provided, `docker_aws_build.sh` and `docker_aws_push.sh` will
attempt to set the `AWS_`-type variables by querying AWS account and
configuration data using the CLI. These two scripts also use
`BUILD_PLATFORM` to set the Docker Compose [`platform`
option](https://docs.docker.com/reference/compose-file/services/#platform)
and can be set to a value other than the default of `linux/amd64` if
targeting other AWS compute instance types (i.e., `linux/arm64`,
etc.).

### Building and Deploying

#### Local Test/Development

Even if targeting AWS, a local development build is a useful "getting
started" exercise. All that is required to build the Docker base, and
"executable" images is:

    # build:
    $ docker_dev_build.sh

    # verify:
    $ docker image ls
    REPOSITORY                                      TAG       IMAGE ID       CREATED          SIZE
    ecco-dataset-production-dev-generate-datasets   latest    aa040dab86dc   4 minutes ago    3.32GB
    ecco-dataset-production-dev-base                latest    94e56150fc7c   5 minutes ago    3.32GB
	...etc...

where `./docker_dev_build.sh` wraps/automates calls to
`../docker-compose.dev.yaml`.

The resulting base image can be run interactively, for example, to
verify that the `ecco_dataset_production` package is "importable", and
that the corresponding command-line utilities can be invoked:

    $ docker run --rm -it ecco-dataset-production-dev-base /bin/bash

    bash-5.2# ls -a /usr/local/bin/edp*
    /usr/local/bin/edp_aws_s3_sync	   /usr/local/bin/edp_create_job_files	    /usr/local/bin/edp_generate_datasets
    /usr/local/bin/edp_create_factors  /usr/local/bin/edp_create_job_task_list
	
	# print command-line utility help, for example:
	bash-5.2# edp_generate_datasets --help

    bash-5.2# python3
    >>> import ecco_dataset_production as edp
    >>> help(edp)

An example demonstrating the use of a local Docker build on data
available locally can be found in
`./demos/SSH_native_latlon_local_docker`.

#### AWS ECR

With the environment setup described above, building, and pushing ECCO
Dataset Production Docker images to AWS ECR can be accomplished, as
noted earlier, using the "build" and "push" scripts that wrap calls to
`../docker-compose.aws.yaml`:

    # AWS ECR build...:
    $ ./docker_aws_build.sh
    ...
    # ...and push:
    $ ./docker_aws_push.sh
    ...
    pushing aws-generate-datasets (this might take awhile)...
    ...done
    pushing aws-base (this might take awhile)...
    ...done

    # verify successful push to AWS ECR:
    $ aws ecr describe-repositories | grep repositoryName | sed 's/.*: //'
    "ecco-dataset-production-aws-base",
    "ecco-dataset-production-aws-generate-datasets",
	...etc...

Docker images that have been pushed to AWS ECR can be used in multiple
ways: they can be pulled into AWS EC2 instances, invoked using AWS
Batch, and paired with AWS "serverless" provisioning mechanisms such
as AWS Fargate and Lambda.  The AWS Batch/Fargate approach is used by
the ECCO central production group and is discussed further in
readthedocs.

If an [EC2](https://aws.amazon.com/ec2/) instance is available, a
useful first-level check of image availability and AWS
[IAM](https://aws.amazon.com/iam/) role/policy settings is to pull the
base image from the ECR and explore the contents interactively from
the EC2 instance, much as was done in the local development case:

    # To see if AWS ECR is visible from the EC2 instance (implicit check on IAM policies):
    $ aws ecr describe-repositories

    $ export AWS_REGION=$(aws configure get region)
    $ export AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
	
    # authenticate. Note that, depending on configuration, Docker commands may 
    # need to be run as superuser (root). Should that be the case, authenticate
    # using "sudo docker":

    $ aws ecr get-login-password | sudo docker login --username AWS --password-stdin ${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com

    # and pull from repo (optionally, set VERSION tag variable):
    $ sudo docker pull ${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/ecco-dataset-production-aws-base:${VERSION:-latest}

    # run interactively to test basic functionality:
    $ sudo docker run --rm -it ecco-dataset-production-aws-base /bin/bash

See the readthdocs.io pages for further discussion regarding AWS
deployment.
