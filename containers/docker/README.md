
## Docker build notes

ECCO Dataset Production Docker images might be built to support
various forms of deployment, though the general use case is that of
supporting ECCO granule generation on AWS. To that end, an AWS-centric
discussion is assumed, and the Dockerfiles have been implemented
assuming Amazon Machine Images (AMI) and related AWS utilities.

### Prerequisites:

It's assumed you already have an AWS account (either a personal
account or one provided through your organization), and that you have
privileges to push Docker images to, and query the contents of, the
AWS Elastic Container Registry (ECR).

Since local Docker builds will be illustrated here (as opposed to
Docker images generated on an EC2 instance; an essentially identical
process), it's assumed that the AWS Command Line Interface has been
installed on your local machine. If not, see
https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-getting-started.html
for details.

Additionally, the following useful environment configuration is assumed:

    # after installing the AWS CLI:
    $ export AWS_REGION=$(aws configure get region)
    $ export AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)

### Local builds:

A local Docker image can be used to verify the build process and to
verify container functionality prior to an AWS ECR push.

To do so, from the repository top-level:

- First, build the base image:

        # generally:
        $ docker image build -t ecco_dataset_production -f ./containers/docker/Dockerfile.base .

        # optionally, if the local machine is based on Apple (Mac OS) silicon
        # and the AWS AMI target is 64 bit Intel/AMD:
        $ docker image build --platform linux/amd64 \
          -t ecco_dataset_production -f ./containers/docker/Dockerfile.base .

- Then, build the "executable" image (wrapper around
  edp\_generate\_dataproducts command):

        # generally:
        $ docker image build -t ecco_edp_generate_dataproducts -f ./containers/docker/Dockerfile.edp_generate_dataproducts .

        # or, as a multi-platorm build:
        $ docker image build --platform linux/amd64 \
          -t ecco_edp_generate_dataproducts -f ./containers/docker/Dockerfile.edp_generate_dataproducts .


Note that the base image can be run interactively to validate all
Python package and command-line functionality (`docker run ... -it
/bin/bash`), while the intent of the executable image is for automated
AWS-based ECCO dataset generation.

### AWS ECR destination build:

In order to push a Docker image to the AWS ECR, a private repository
destination needs to exist, and general AWS ECR image file naming
conventions need to be followed.

If a private repository does not yet exist, one can be created using
the AWS Console by following the instructions at:
`https://<aws_region>.console.aws.amazon.com/ecr/private-registry/repositories/create?region=<aws_region>`,
where `<region>` is your AWS account (or other intended) region (e.g.,
us-west-1, us-east-2, etc.).

To build Docker images that are to be pushed to the AWS ECR:

- Build base image as before, although if intending to push to the AWS
  ECR, the following naming conventions should be followed:

        $ docker image build \
          -t ${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/ecco_dataset_production:latest \
          -f ./containers/docker/Dockerfile.base .

- Build, the "executable" image:

        $ docker image build \
          -t ${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/ecco_edp_generate_dataproducts:latest \
          -f ./containers/docker/Dockerfile.edp_generate_dataproducts .

Note that, as before, if building on Apple silicon and running on an
Intel/AMD 64 bit AWS AMI, the platform flag, e.g., `--platform
linux/amd64` should be used.

To push to the ECR using the AWS Command Line Interface:

    $ aws ecr get-login-password | docker login --username AWS --password-stdin ${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com
    $ docker push ${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/ecco_edp_generate_dataproducts:latest

A successful push can be verified using the AWS CLI:

    $ aws ecr list-images --repository-name <repository-name>

where `<repository-name>` is the name given when the repository was created, above, on the AWS Console.

### Pulling the image from AWS ECR to an EC2 instance:

In addition to testing the pushed image locally (which is always a good idea), the image can also be tested on an EC2 instance,
which is also an implicit test of AWS IAM policies for the EC2 instance.

- From the EC2 instance:

        # To see if AWS ECR is visible from the EC2 instance (implicit check on IAM policies):
        $ aws ecr describe-repositories

        # for ease-of-use, set AWS_ACCOUNT_ID and AWS_REGION as above

        # authenticate, as above using `aws ecr get-login-password ...`. Note
        # that, depending on the installation, all docker commands may need to
        # be run as superuser (root). Should that be the case, authenticate
        # using "sudo docker":

        $ aws ecr get-login-password | sudo docker login --username AWS --password-stdin ${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com

        # and pull from repo:
        $ sudo docker pull ${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/ecco_edp_generate_dataproducts

        # and run using, e.g.:
        $ sudo docker run --rm --env TASKLIST=s3://<bucket_and_prefix>/some_task_list.json ecco_edp_generate_dataproducts
        # where TASKLIST must reference an S3-resident task list


